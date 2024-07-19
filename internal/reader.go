package internal

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/murakmii/retsu/thrift/parquet"
)

type (
	Reader[T any] struct {
		par *Parquet
		st  *Structure
	}
)

func NewReader[T any](ctx context.Context, par *Parquet) (*Reader[T], error) {
	st, err := par.Inspect(ctx)
	if err != nil {
		return nil, err
	}

	return &Reader[T]{par: par, st: st}, nil
}

func (r *Reader[T]) Aggregate(
	path string,
	decoder func([]byte) (T, []byte),
	aggregator Aggregator[T],
) error {
	schema := r.st.FindSchema(path)
	if schema == nil || !schema.IsLeaf() {
		return fmt.Errorf("'%s' column does not exist", path)
	}

	for _, col := range r.st.FindColumnChunk(path) {
		dict, err := r.readDict(col, decoder)
		if err != nil {
			return fmt.Errorf("failed to read dictionary page: %w", err)
		}

		for _, dataPage := range col.DataPages() {
			if err := r.readDataPage(schema, col, dict, dataPage, decoder, aggregator); err != nil {
				return fmt.Errorf("failed to read data page: %w", err)
			}
		}
	}

	return nil
}

func (r *Reader[T]) readDict(col *ColumnChunk, decoder func([]byte) (T, []byte)) ([]T, error) {
	if !col.HasDict() {
		return nil, nil
	}

	dictPage := col.DictPage()
	pageContent, err := r.readPageContent(col, dictPage)
	if err != nil {
		return nil, err
	}

	dict := make([]T, dictPage.NumValues)
	for i := 0; i < int(dictPage.NumValues); i++ {
		dict[i], pageContent = decoder(pageContent)
	}

	if len(pageContent) != 0 {
		return nil, fmt.Errorf("invalid dictionary page length")
	}

	return dict, nil
}

func (r *Reader[T]) readDataPage(
	schema *Schema,
	col *ColumnChunk,
	dict []T,
	page *Page,
	decoder func([]byte) (T, []byte),
	aggregator Aggregator[T],
) error {
	if page.Data.RepetitionLevelEncoding != parquet.Encoding_RLE {
		return fmt.Errorf("unsupported repetition level encoding: %s", page.Data.RepetitionLevelEncoding)
	}
	if page.Data.DefinitionLevelEncoding != parquet.Encoding_RLE {
		return fmt.Errorf("unsupported definition level encoding: %s", page.Data.DefinitionLevelEncoding)
	}

	data, err := r.readPageContent(col, page)
	if err != nil {
		return nil
	}

	if schema.HasRepetitionLevels() {
		data = skipLevel(data)
	}
	if schema.HasDefinitionLevels() {
		data = skipLevel(data)
	}

	switch page.Encoding {
	case parquet.Encoding_RLE_DICTIONARY:
		readRLEDictionary(dict, data, aggregator)
	default:
		return fmt.Errorf("unsupported page encoding: %s", page.Encoding)
	}

	return nil
}

func (r *Reader[T]) readPageContent(col *ColumnChunk, page *Page) ([]byte, error) {
	var size int32
	if col.Codec == parquet.CompressionCodec_UNCOMPRESSED {
		size = page.UncompressedSize
	} else {
		size = page.CompressedSize
	}

	content, err := r.par.Read(page.Offset, int64(size))
	if err != nil {
		return nil, fmt.Errorf("failed to read page content: %w", err)
	}

	switch col.Codec {
	case parquet.CompressionCodec_UNCOMPRESSED:
		return content, nil

	case parquet.CompressionCodec_ZSTD:
		return zstd.Decompress(make([]byte, page.UncompressedSize), content)

	default:
		return nil, fmt.Errorf("unsupported compression codec %s", col.Codec)
	}
}

func readRLEDictionary[T any](dict []T, data []byte, aggregator Aggregator[T]) {
	bitWidth := data[0]
	readRLE(data[1:], uint32(bitWidth), func(value uint32, repeated uint64) {
		aggregator.Aggregate(dict[value], repeated)
	})
}

func readRLE(data []byte, bitWidth uint32, callback func(uint32, uint64)) {
	mask := uint32(1<<bitWidth) - 1
	byteWidth := int((bitWidth + 7) / 8)
	var header uint64

	for len(data) > 0 {
		header, data = readULEB128(data)
		isBitPacked := (header & 0x01) == 1
		header >>= 1

		if !isBitPacked {
			var runLenValue uint32
			for i := 0; i < byteWidth; i++ {
				runLenValue |= uint32(data[i]) << (i * 8)
			}
			data = data[byteWidth:]

			callback(runLenValue, header)
			continue
		}

		var unpacked, unpackedBits uint32
		for i := header * 8; i > 0; {
			unpacked |= uint32(data[0]) << unpackedBits
			unpackedBits += 8
			data = data[1:]

			for ; unpackedBits >= bitWidth; unpackedBits -= bitWidth {
				callback(unpacked&mask, 1)
				unpacked >>= bitWidth
				i--
			}
		}
	}
}

func readULEB128(data []byte) (uint64, []byte) {
	var ret uint64

	for i := 0; ; i++ {
		b := data[0]
		data = data[1:]
		ret |= uint64(b&0x7F) << uint(i*7)
		if b&0x80 == 0 {
			break
		}
	}

	return ret, data
}

func skipLevel(data []byte) []byte {
	levelLen := binary.LittleEndian.Uint32(data)
	return data[4+levelLen:]
}
