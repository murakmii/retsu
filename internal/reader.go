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
	if schema == nil || *schema.Type != parquet.Type_INT64 {
		return 0, fmt.Errorf("'%s' column(type: int64) does not exist", path)
	}

	var sum int64
	for _, col := range r.meta.FindColumnChunk(path) {
		if err := r.par.Seek(col.PageHeadOffset()); err != nil {
			return 0, err
		}

		fmt.Println("* Next column chunk")

		for {
			offset, err := r.par.CurrentOffset()
			if err != nil {
				return 0, err
			}

			if offset == col.PageTailOffset() {
				break
			}

			header, data, err := r.readCurrentPage(ctx, col.Codec)
			if err != nil {
				return 0, err
			}

			fmt.Printf("Page type: %s, size: %d\n", header.Type, len(data))

			if header.DataPageHeader != nil {
				fmt.Printf("Data encoding: %s\n", header.DataPageHeader.Encoding)
			}
		}
	}

	return sum, nil
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

func (r *Reader) readDataPage(
	ctx context.Context,
	schema *Schema,
	codec parquet.CompressionCodec,
	dict []int64,
) (int64, error) {
	// データページのページヘッダーと値の領域を取得
	header, data, err := r.readCurrentPage(ctx, codec)
	if err != nil {
		return 0, err
	}

	// 反復レベルがエンコードされているなら、それをスキップ
	if schema.Depth > 1 {
		data = skipLevel(data)
	}

	// 定義レベルがエンコードされているなら、それをスキップ
	if *schema.RepetitionType != parquet.FieldRepetitionType_REQUIRED {
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

// RLEのデコード
// 第二引数はRLEでエンコード対象となっている値のビット幅
// 第三引数は、値がデコードされる度に呼び出される関数で、
// この関数の第一引数は値自体、第二引数は値の繰り返し回数
func readRLE(data []byte, bitWidth uint32, callback func(uint32, uint64)) {
	mask := uint32(1<<bitWidth) - 1
	byteWidth := int((bitWidth + 7) / 8)
	var header uint64

	for len(data) > 0 {
		// ヘッダーを読み取り下位1ビット目でBit-Packingかどうかを判断
		// 判断したら1ビット右シフトして本来のヘッダーの値に直しておく
		header, data = readULEB128(data)
		isBitPacked := (header & 0x01) == 1
		header >>= 1

		// Bit-Packingではない、つまりRun Length Encodigでのデコード
		if !isBitPacked {
			// 値のビット幅に対して十分なバイト幅分だけ読み取り、
			// リトルエンディアンとして値を復元
			var runLenValue uint32
			for i := 0; i < byteWidth; i++ {
				runLenValue |= uint32(data[i]) << (i * 8)
			}
			data = data[byteWidth:]

			// デコードした値と、ヘッダーの値を繰り返し回数として関数呼び出し
			callback(runLenValue, header)
			continue
		}

		// Bit-Packingによるデコード
		var unpacked, unpackedBits uint32
		for i := header * 8; i > 0; { // 8乗じた値が本来の値の数
			// 1バイト読み取って、unpackedの上位に論理和していく
			unpacked |= uint32(data[0]) << unpackedBits
			unpackedBits += 8
			data = data[1:]

			// unpacked上に読み込まれたビット数(unpackedBits)が値のビット幅(bitWidth)を超えている分だけ、
			// デコードした値と見なし関数呼び出し。関数に渡した分は左シフトしてunpackedから追い出す
			for ; unpackedBits >= bitWidth; unpackedBits -= bitWidth {
				callback(unpacked&mask, 1)
				unpacked >>= bitWidth
				i--
			}
		}
	}
}

// ULEB-128のデコード
// 上位8ビット目が1の1バイトが見つかるまで、
// 上位8ビット目以外の下位7ビットをuint64型の変数に論理和していきデコードする
// ULEB-128は可変長なので、デコードの結果残った部分を、デコード結果の整数と共に返す
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
