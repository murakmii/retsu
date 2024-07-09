package internal

import "github.com/murakmii/retsu/thrift/parquet"

// Parquetファイルの構造を表すための一連の構造体
type (
	Structure struct {
		RootField *Schema     `json:"root_field"`
		RowGroups []*RowGroup `json:"row_groups"`
	}

	Schema struct {
		Name   string        `json:"name"`
		Type   *parquet.Type `json:"type,omitempty"`
		Nested []*Schema     `json:"nested,omitempty"`
	}

	RowGroup struct {
		NumRows int64          `json:"num_rows"`
		Columns []*ColumnChunk `json:"columns"`
	}

	ColumnChunk struct {
		Path      string                   `json:"path"`
		Type      parquet.Type             `json:"type"`
		Codec     parquet.CompressionCodec `json:"codec,omitempty"`
		NumValues int64                    `json:"num_values"`
		Pages     []*Page                  `json:"pages,omitempty"`
	}

	Page struct {
		Type             parquet.PageType `json:"type"`
		UncompressedSize int32            `json:"uncompressed_size"`
		CompressedSize   int32            `json:"compressed_size"`
		Offset           int64            `json:"offset"`
		NumValues        int32            `json:"num_values"`
		Encoding         parquet.Encoding `json:"encoding"`
	}
)
