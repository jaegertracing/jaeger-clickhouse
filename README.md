# Jaeger ClickHouse

This is implementation of Jaeger ClickHouse [storage plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc).
See as well [jaegertracing/jaeger/issues/1438](https://github.com/jaegertracing/jaeger/issues/1438) for historical discussion regarding Clickhouse plugin.

## Documentation

Refer to the [config.yaml](./config.yaml) for all supported configuration options.

* [Kubernetes deployment documentation](./kubernetes.md)
* [Sharding and replicaion](./sharding-and-replication.md)

## Build & Run

### Docker database example

```bash
docker run --rm -it -p9000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 yandex/clickhouse-server:21
GOOS=linux make build run
make run-hotrod
```

Open [localhost:16686](http://localhost:16686) and [localhost:8080](http://localhost:8080).

### Custom database

You need to specify connection options in config.yaml file, then you can run 

```bash
make build
SPAN_STORAGE_TYPE=grpc-plugin {Jaeger binary adress} --query.ui-config=jaeger-ui.json --grpc-storage-plugin.binary=./{name of built binary} --grpc-storage-plugin.configuration-file=config.yaml --grpc-storage-plugin.log-level=debug
```

## Credits

This project is based on https://github.com/bobrik/jaeger/tree/ivan/clickhouse/plugin/storage/clickhouse.
