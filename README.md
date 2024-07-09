# retsu

Toy implementation to learn Apache Parquet.

## Development

### Generate code to decode data in Parquet encoded by Thrift Compact protocol

```shell
cd thrift

# Download Thrift definition file for Parquet
curl -o parquet.thrift https://raw.githubusercontent.com/apache/parquet-format/apache-parquet-format-2.10.0/src/main/thrift/parquet.thrift

# Build Thrift compiler(Docker container image) and generate code
rm parquet/*
docker build -t thrift .
docker run --rm -u `id -u`:`id -g` -v .:/retsu -t thrift -out /retsu j --gen go /retsu/parquet.thrift
```

### Run inspect command

```shell
# TLC Trip Record Data provided by nyc.gov is good sample of parquet file.
# https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

$ go run cmd/main.go inspect --path taxi.parquet | jq '.row_groups[0].columns[] | select(.path == "passenger_count")'
{
  "path": "passenger_count",
  "type": "INT64",
  "codec": "ZSTD",
  "num_values": 1048576,
  "pages": [
    {
      "type": "DICTIONARY_PAGE",
      "uncompressed_size": 72,
      "compressed_size": 41,
      "offset": 9162854,
      "num_values": 9,
      "encoding": "PLAIN"
    },
    {
      "type": "DATA_PAGE",
      "uncompressed_size": 464942,
      "compressed_size": 198241,
      "offset": 9162921,
      "num_values": 1048576,
      "encoding": "RLE_DICTIONARY"
    }
  ]
}
```