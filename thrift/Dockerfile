FROM buildpack-deps:bullseye

RUN curl -o thrift.tar.gz https://dlcdn.apache.org/thrift/0.20.0/thrift-0.20.0.tar.gz && tar -xf thrift.tar.gz && mv thrift-0.20.0 /thrift
WORKDIR /thrift

RUN ./configure --disable-tutorial --disable-libs && make && make install

ENTRYPOINT ["/usr/local/bin/thrift"]
