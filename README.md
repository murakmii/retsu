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