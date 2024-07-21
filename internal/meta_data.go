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
		Path      string                   `json:"path"`
		Codec     parquet.CompressionCodec `json:"codec,omitempty"`
		NumValues int64                    `json:"num_values"`
		Pages     []*Page                  `json:"pages,omitempty"`
	}

	Page struct {
		Type      parquet.PageType `json:"type"`
		Size      int32            `json:"size"`
		Offset    int64            `json:"offset"`
		NumValues int32            `json:"num_values"`
		Encoding  parquet.Encoding `json:"encoding"`
		Data      *DataPage        `json:"data,omitempty"`
	}

	DataPage struct {
		RepetitionLevelEncoding parquet.Encoding
		DefinitionLevelEncoding parquet.Encoding
	}
)

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

func (schema *Schema) HasRepetitionLevels() bool {
	return schema.Depth > 1
}

func (schema *Schema) HasDefinitionLevels() bool {
	return schema.RepetitionType != nil && *schema.RepetitionType != parquet.FieldRepetitionType_REQUIRED
}

func (col *ColumnChunk) HasDict() bool {
	return len(col.Pages) > 0 && col.Pages[0].Type == parquet.PageType_DICTIONARY_PAGE
}

func (col *ColumnChunk) DictPage() *Page {
	if col.HasDict() {
		return col.Pages[0]
	} else {
		return nil
	}
}

func (col *ColumnChunk) DataPages() []*Page {
	dataPages := make([]*Page, 0)
	for _, page := range col.Pages {
		if page.Type == parquet.PageType_DATA_PAGE {
			dataPages = append(dataPages, page)
		}
	}
	return dataPages
}
