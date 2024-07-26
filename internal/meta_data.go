package internal

import (
	"github.com/murakmii/retsu/thrift/parquet"
	"strings"
)

// Parquetファイルの構造を表すための一連の構造体
type (
	MetaData struct {
		SchemaTree *Schema     `json:"schema_tree"`
		TotalRows  int64       `json:"total_rows"`
		RowGroups  []*RowGroup `json:"row_groups"`
	}

	Schema struct {
		Name           string                       `json:"name"`
		Type           *parquet.Type                `json:"type,omitempty"`
		TypeLength     *int32                       `json:"type_length,omitempty"`
		RepetitionType *parquet.FieldRepetitionType `json:"repetition_type"`
		Children       map[string]*Schema           `json:"children,omitempty"`
		Depth          int                          `json:"depth"`
	}

	RowGroup struct {
		NumRows int64          `json:"num_rows"`
		Columns []*ColumnChunk `json:"columns"`
	}

	ColumnChunk struct {
		Path                  string                   `json:"path"`
		Codec                 parquet.CompressionCodec `json:"codec,omitempty"`
		NumValues             int64                    `json:"num_values"`
		TotalUncompressedSize int64                    `json:"total_uncompressed_size"`
		TotalCompressedSize   int64                    `json:"total_compressed_size"`
		DataPageOffset        int64                    `json:"data_page_offset"`
		DictPageOffset        *int64                   `json:"dict_page_offset"`
	}
)

// 列の名前を指定してスキーマ情報を取得
func (s *MetaData) FindSchema(path string) *Schema {
	schema := s.SchemaTree
	var ok bool

	for _, p := range strings.Split(path, ".") {
		if schema.Children == nil {
			return nil
		}
		if schema, ok = schema.Children[p]; !ok {
			return nil
		}
	}

	return schema
}

// 列の名前を指定して列チャンクを取得
func (s *MetaData) FindColumnChunk(path string) []*ColumnChunk {
	columns := make([]*ColumnChunk, 0)

	for _, row := range s.RowGroups {
		for _, col := range row.Columns {
			if col.Path == path {
				columns = append(columns, col)
			}
		}
	}

	return columns
}

func (schema *Schema) IsLeaf() bool {
	return schema.Type != nil
}

func (col *ColumnChunk) HasDict() bool {
	return col.DictPageOffset != nil
}

func (col *ColumnChunk) PageHeadOffset() int64 {
	if col.HasDict() {
		return *col.DictPageOffset
	} else {
		return col.DataPageOffset
	}
}

func (col *ColumnChunk) PageTailOffset() int64 {
	if col.Codec == parquet.CompressionCodec_UNCOMPRESSED {
		return col.PageHeadOffset() + col.TotalUncompressedSize
	} else {
		return col.PageHeadOffset() + col.TotalCompressedSize
	}
}

func (schema *Schema) HasRepetitionLevels() bool {
	return schema.Depth > 1
}

func (schema *Schema) HasDefinitionLevels() bool {
	return schema.RepetitionType != nil && *schema.RepetitionType != parquet.FieldRepetitionType_REQUIRED
}
