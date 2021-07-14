# Jaeger ClickHouse

Jaeger ClickHouse gRPC [storage plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc).

This is WIP and it is based on https://github.com/bobrik/jaeger/tree/ivan/clickhouse/plugin/storage/clickhouse. 
See as well [jaegertracing/jaeger/issues/1438](https://github.com/jaegertracing/jaeger/issues/1438) for ClickHouse plugin.

## Build

```bash
make build
```

## Run 

```bash
docker run --rm -it -p9000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server
# download Jaeger all-in-one
JAEGER_ALL_IN_ONE=<path to all-in-one> make run
```
