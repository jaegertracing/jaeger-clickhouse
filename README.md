# Jaeger ClickHouse

Jaeger ClickHouse gRPC [storage plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc).

This is WIP and it is based on https://github.com/bobrik/jaeger/tree/ivan/clickhouse/plugin/storage/clickhouse. 
See as well [jaegertracing/jaeger/issues/1438](https://github.com/jaegertracing/jaeger/issues/1438) for ClickHouse plugin.

## Connection to remote database

You can specify connection settings in ``config.yaml`` file

### Using TLS connection

For TLS connection, you need to put CA-certificate to project directory and specify path to it in ``config.yaml`` 
file using ``/data/{path to certificate from project directory}``

## Build & Run

### Docker database example

```bash
docker run --rm -it -p9000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server:21
GOOS=linux make build run
make run-hotrod
```

Open [localhost:16686](http://localhost:16686) and [localhost:8080](http://localhost:8080).

### Custom database

You need to specify connection options in ``config.yaml`` file, then run 
```bash
GOOS=linux make build run
```
At this point, you can enter Jaeger UI at [localhost:16686](http://localhost:16686).

Then you can try using Jaeger with HotROD by running
```bash
make run-hotrod
```
Then you should open [localhost:8080](http://localhost:8080).

