package internal

import (
	"context"
	"encoding/binary"
	"fmt"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/murakmii/retsu/thrift/parquet"
	"io"
	"strings"
)

type (
	// Parquetファイル
	Parquet struct {
		r     io.ReadSeeker // TProtocolだけではシーク等ができないので元の ReadSeeker も保持
		proto thrift.TProtocol
	}

	ThriftStruct interface {
		Read(ctx context.Context, protocol thrift.TProtocol) error
	}
)

func NewParquet(r io.ReadSeeker) *Parquet {
	return &Parquet{
		r: r,
		proto: thrift.NewTCompactProtocolConf(
			&thrift.StreamTransport{Reader: r},
			nil,
		),
	}
}

// Parquetファイルを解析し、スキーマ等のメタデータを返す
func (par *Parquet) Inspect(ctx context.Context) (*MetaData, error) {
	// ファイル末尾から8バイト戻った位置にある、フッター長を取得する
	if _, err := par.r.Seek(-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek to footer length: %w", err)
	}

	footerLen := make([]byte, 4)
	if _, err := io.ReadFull(par.r, footerLen); err != nil {
		return nil, fmt.Errorf("failed to read footer length: %w", err)
	}

	// フッター、フッター長、マジックナンバー分だけ末尾から先頭、つまりフッターの先頭にシークする
	if _, err := par.r.Seek(-int64(binary.LittleEndian.Uint32(footerLen))-8, io.SeekEnd); err != nil {
		return nil, fmt.Errorf("failed to seek to footer: %w", err)
	}

	// フッターを読み取り、専用の構造体に変換していく
	footer := &parquet.FileMetaData{}
	if err := par.ReadThrift(ctx, footer); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	return par.inspectFooter(ctx, footer)
}

// フッターからのメタデータの取得
func (par *Parquet) inspectFooter(ctx context.Context, footer *parquet.FileMetaData) (*MetaData, error) {
	metaData := &MetaData{
		TotalRows: footer.NumRows,
		RowGroups: make([]*RowGroup, len(footer.RowGroups)),
	}
	metaData.SchemaTree, _ = inspectSchema(footer.Schema, 0) // スキーマ情報を変換

	// 行グループ毎に変換
	for i := 0; i < len(footer.RowGroups); i++ {
		metaData.RowGroups[i] = &RowGroup{
			NumRows: footer.RowGroups[i].NumRows,
			Columns: make([]*ColumnChunk, len(footer.RowGroups[i].Columns)),
		}

		// 列チャンク毎に変換
		for j := 0; j < len(footer.RowGroups[i].Columns); j++ {
			col := footer.RowGroups[i].Columns[j]

			metaData.RowGroups[i].Columns[j] = &ColumnChunk{
				Path:                  strings.Join(col.MetaData.PathInSchema, "."),
				Codec:                 col.MetaData.Codec,
				NumValues:             col.MetaData.NumValues,
				TotalUncompressedSize: col.MetaData.TotalUncompressedSize,
				TotalCompressedSize:   col.MetaData.TotalCompressedSize,
				DataPageOffset:        col.MetaData.DataPageOffset,
				DictPageOffset:        col.MetaData.DictionaryPageOffset,
			}
		}
	}

	return metaData, nil
}

// スキーマ情報の変換
// リストに均された一連のスキーマ用構造体から、木構造のスキーマを復元して返す
// 戻り値として、木構造に復元されたスキーマの親又は根となる単一の構造体と、
// 木構造に復元されていない残りのリストを返す
func inspectSchema(elements []*parquet.SchemaElement, depth int) (*Schema, []*parquet.SchemaElement) {
	// リストの先頭を木構造のノードとしてSchema構造体に
	s := &Schema{
		Name:           elements[0].Name,
		Type:           elements[0].Type,
		RepetitionType: elements[0].RepetitionType,
		Depth:          depth,
	}

	// num_childrenが無いなら木構造の葉なので子については考えず、リストの先頭以外を未処理として返す
	if !elements[0].IsSetNumChildren() {
		return s, elements[1:]
	}

	numChildren := elements[0].GetNumChildren()
	s.Children = make(map[string]*Schema, numChildren)
	elements = elements[1:]

	// 子となるnum_children分のスキーマについて、本関数を再帰的に呼び出して木構造を復元してく
	var child *Schema
	for i := int32(0); i < numChildren; i++ {
		// 再帰的に処理した際、リストの要素のうちいくつが処理されるかは呼び出し時点では分からないので、
		// 二番目の戻り値でリストを更新する
		child, elements = inspectSchema(elements, depth+1)
		s.Children[child.Name] = child
	}

	return s, elements
}

func (par *Parquet) Seek(offset int64) error {
	if _, err := par.r.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("failed to seek parquet file(offset: %d): %w)", offset, err)
	}

	return nil
}

func (par *Parquet) CurrentOffset() (int64, error) {
	offset, err := par.r.Seek(0, io.SeekCurrent)
	if err != nil {
		return -1, fmt.Errorf("failed to get current offset of parquet file: %w)", err)
	}

	return offset, nil
}

func (par *Parquet) ReadThrift(ctx context.Context, t ThriftStruct) error {
	return t.Read(ctx, par.proto)
}

func (par *Parquet) Read(size int64) ([]byte, error) {
	buf := make([]byte, size)
	if _, err := io.ReadFull(par.r, buf); err != nil {
		return nil, fmt.Errorf("failed to read parquet file(size: %d): %w)", size, err)
	}

	return buf, nil
}
