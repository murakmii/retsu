package internal

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/DataDog/zstd"
	"github.com/murakmii/retsu/thrift/parquet"
)

type (
	Reader struct {
		par  *Parquet
		meta *MetaData
	}
)

func NewReader(ctx context.Context, par *Parquet) (*Reader, error) {
	meta, err := par.Inspect(ctx)
	if err != nil {
		return nil, err
	}

	return &Reader{par: par, meta: meta}, nil
}

func (r *Reader) SumInt64(ctx context.Context, path string) (int64, error) {
	schema := r.meta.FindSchema(path)
	if schema == nil || !schema.IsLeaf() {
		return 0, fmt.Errorf("'%s' column does not exist", path)
	}

	var sum int64
	for _, col := range r.meta.FindColumnChunk(path) {
		if err := r.par.Seek(col.PageHeadOffset()); err != nil {
			return 0, err
		}

		var err error
		var dict []int64

		if col.HasDict() {
			dict, err = r.readDict(ctx, col)
			if err != nil {
				return 0, fmt.Errorf("failed to read dictionary page: %w", err)
			}
		}

		for {
			offset, err := r.par.CurrentOffset()
			if err != nil {
				return 0, err
			}

			if offset == col.PageTailOffset() {
				break
			}

			s, err := r.readDataPage(ctx, schema, col.Codec, dict)
			if err != nil {
				return 0, fmt.Errorf("failed to read data page: %w", err)
			}

			sum += s
		}
	}

	return sum, nil
}

func (r *Reader) readDict(ctx context.Context, col *ColumnChunk) ([]int64, error) {
	header, data, err := r.readCurrentPage(ctx, col.Codec)
	if err != nil {
		return nil, err
	}

	dict := make([]int64, header.DictionaryPageHeader.NumValues)
	for i := 0; i < len(dict); i++ {
		dict[i] = int64(binary.LittleEndian.Uint64(data))
		data = data[8:]
	}

	return dict, nil
}

func (r *Reader) readCurrentPage(ctx context.Context, codec parquet.CompressionCodec) (*parquet.PageHeader, []byte, error) {
	header := &parquet.PageHeader{}
	if err := r.par.ReadThrift(ctx, header); err != nil {
		return nil, nil, err
	}

	var size int32
	if codec == parquet.CompressionCodec_UNCOMPRESSED {
		size = header.UncompressedPageSize
	} else {
		size = header.CompressedPageSize
	}

	data, err := r.par.Read(int64(size))
	if err != nil {
		return nil, nil, err
	}

	switch codec {
	case parquet.CompressionCodec_ZSTD:
		data, err = zstd.Decompress(nil, data)

	case parquet.CompressionCodec_UNCOMPRESSED:
	// nop
	default:
		return nil, nil, fmt.Errorf("unsupported compression codec %s", codec)
	}

	return header, data, nil
}

func (r *Reader) readDataPage(
	ctx context.Context,
	schema *Schema,
	codec parquet.CompressionCodec,
	dict []int64,
) (int64, error) {
	header, data, err := r.readCurrentPage(ctx, codec)
	if err != nil {
		return 0, err
	}

	if header.DataPageHeader.RepetitionLevelEncoding != parquet.Encoding_RLE {
		return 0, fmt.Errorf("unsupported repetition level encoding: %s", header.DataPageHeader.RepetitionLevelEncoding)
	}
	if header.DataPageHeader.DefinitionLevelEncoding != parquet.Encoding_RLE {
		return 0, fmt.Errorf("unsupported definition level encoding: %s", header.DataPageHeader.DefinitionLevelEncoding)
	}

	if schema.HasRepetitionLevels() {
		data = skipLevel(data)
	}
	if schema.HasDefinitionLevels() {
		data = skipLevel(data)
	}

	var sum int64

	switch header.DataPageHeader.Encoding {
	case parquet.Encoding_RLE_DICTIONARY:
		bitWidth := data[0]
		readRLE(data[1:], uint32(bitWidth), func(index uint32, repeated uint64) {
			sum += dict[index] * int64(repeated)
		})

	default:
		return 0, fmt.Errorf("unsupported page encoding: %s", header.DataPageHeader.Encoding)
	}

	return sum, nil
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
