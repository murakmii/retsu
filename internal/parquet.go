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

// Parquetファイルをデコードしていくための構造体
type Parquet struct {
	r     io.ReadSeeker // TProtocolだけではシーク等ができないので元の ReadSeeker も保持
	proto thrift.TProtocol
}

func NewParquet(r io.ReadSeeker) *Parquet {
	return &Parquet{
		r: r,
		proto: thrift.NewTCompactProtocolConf(
			&thrift.StreamTransport{Reader: r},
			nil,
		),
	}
}

// Parquetファイルを解析し、スキーマ等の構造を返す
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
	if err := footer.Read(ctx, par.proto); err != nil {
		return nil, fmt.Errorf("failed to read footer: %w", err)
	}

	return par.inspectFooter(ctx, footer)
}

func (par *Parquet) inspectFooter(ctx context.Context, footer *parquet.FileMetaData) (*MetaData, error) {
	metaData := &MetaData{RowGroups: make([]*RowGroup, len(footer.RowGroups))}
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

			// ページを変換
			pages, err := par.inspectPages(ctx, col)
			if err != nil {
				return nil, fmt.Errorf("failed to inspect page of row=%d, col=%d: %w", i, j, err)
			}

			metaData.RowGroups[i].Columns[j] = &ColumnChunk{
				Path:      strings.Join(col.MetaData.PathInSchema, "."),
				Codec:     col.MetaData.Codec,
				NumValues: col.MetaData.NumValues,
				Pages:     pages,
			}
		}
	}

	return metaData, nil
}

// スキーマ情報の変換
func inspectSchema(elements []*parquet.SchemaElement, depth int) (*Schema, []*parquet.SchemaElement) {
	s := &Schema{
		Name:           elements[0].Name,
		Type:           elements[0].Type,
		TypeLength:     elements[0].TypeLength,
		RepetitionType: elements[0].RepetitionType,
		Depth:          depth,
	}

	// NumChildren を持つフィールドは、後続の NumChildren 個のフィールドをネストしたフィールドとして扱う
	// NumChildren を持たないならネストしたフィールドは存在しないので、この時点で処理を返す
	if !elements[0].IsSetNumChildren() {
		return s, elements[1:]
	}

	numChildren := elements[0].GetNumChildren()
	s.Children = make(map[string]*Schema, numChildren)
	elements = elements[1:]

	// ネストしたフィールドがさらにネストしていることもあるので、
	// それぞれについて再帰的に処理する
	var child *Schema
	for i := int32(0); i < numChildren; i++ {
		child, elements = inspectSchema(elements, depth+1)
		s.Children[child.Name] = child
	}

	return s, elements
}

// ページ情報の変換
func (par *Parquet) inspectPages(ctx context.Context, col *parquet.ColumnChunk) ([]*Page, error) {
	// ページ群の先頭オフセットを求めシークする
	// 辞書ページがあるならそのオフセット、辞書ページを持たないならデータページのオフセットが先頭になる
	var offset int64
	if col.MetaData.IsSetDictionaryPageOffset() {
		offset = *col.MetaData.DictionaryPageOffset
	} else {
		offset = col.MetaData.DataPageOffset
	}

	if _, err := par.r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek to head of pages(%d): %w", offset, err)
	}

	// ページ群の終端のオフセットを求める
	// 列チャンクより、ページ内容が圧縮されているなら TotalCompressedSize
	// 圧縮されていないなら TotalUncompressedSize をページ群の先頭オフセットに加算した位置が終端となる
	endOfPages := offset
	isCompressed := col.MetaData.Codec != parquet.CompressionCodec_UNCOMPRESSED
	if isCompressed {
		endOfPages += col.MetaData.TotalCompressedSize
	} else {
		endOfPages += col.MetaData.TotalUncompressedSize
	}

	pages := make([]*Page, 0)
	var page *Page
	var err error

	// 1ページずつ読み取り、ページ群の終端に移動した時点で終了
	for offset != endOfPages {
		page, offset, err = par.inspectNextPage(ctx, isCompressed)
		if err != nil {
			return nil, fmt.Errorf("failed to inspect page(%d): %w", offset, err)
		}

		pages = append(pages, page)
	}

	return pages, nil
}

func (par *Parquet) inspectNextPage(ctx context.Context, isCompressed bool) (*Page, int64, error) {
	header := &parquet.PageHeader{}
	if err := header.Read(ctx, par.proto); err != nil {
		return nil, 0, fmt.Errorf("failed to read page header: %w", err)
	}

	// 後からページ内容に簡単にアクセスできるよう、ページヘッダー読み取り後のオフセットを記録しておく
	offset, err := par.r.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to get current offset: %w", err)
	}

	page := &Page{Type: header.Type, Offset: offset}
	if isCompressed {
		page.Size = header.CompressedPageSize
	} else {
		page.Size = header.UncompressedPageSize
	}

	switch page.Type {
	case parquet.PageType_DATA_PAGE:
		page.NumValues = header.DataPageHeader.NumValues
		page.Encoding = header.DataPageHeader.Encoding
		page.Data = &DataPage{
			RepetitionLevelEncoding: header.DataPageHeader.RepetitionLevelEncoding,
			DefinitionLevelEncoding: header.DataPageHeader.DefinitionLevelEncoding,
		}

	case parquet.PageType_DICTIONARY_PAGE:
		page.NumValues = header.DictionaryPageHeader.NumValues
		page.Encoding = header.DictionaryPageHeader.Encoding

	default:
		// nop
	}

	// この時点ではページ内容をデコードしなくても良いのでシークして読み飛ばす
	offset += int64(page.Size)
	if _, err := par.r.Seek(offset, io.SeekStart); err != nil {
		return nil, 0, fmt.Errorf("failed to seek to next page(%d): %w", offset, err)
	}

	return page, offset, nil
}

func (par *Parquet) Read(offset, size int64) ([]byte, error) {
	if _, err := par.r.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek parquet file(offset: %d, size: %d): %w)", offset, size, err)
	}

	buf := make([]byte, size)
	if _, err := io.ReadFull(par.r, buf); err != nil {
		return nil, fmt.Errorf("failed to read parquet file(offset: %d, size: %d): %w)", offset, size, err)
	}

	return buf, nil
}
