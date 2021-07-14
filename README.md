# Jaeger ClickHouse

Jaeger ClickHose gRPC [storage plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc).

This is project is based on https://github.com/bobrik/jaeger/tree/ivan/clickhouse/plugin/storage/clickhouse

## Build

```bash
make build
```

## Run 

```bash
docker run --rm -it -p9000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server
SPAN_STORAGE_TYPE=grpc-plugin ./all-in-one --grpc-storage-plugin.binary=/home/ploffay/projects/jaegertracing/jaeger-clickhouse/jaeger-clickhouse
```
