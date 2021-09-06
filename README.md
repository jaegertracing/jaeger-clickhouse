# Jaeger ClickHouse

This is implementation of Jaeger's [storage plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc) for ClickHouse.
See as well [jaegertracing/jaeger/issues/1438](https://github.com/jaegertracing/jaeger/issues/1438) for historical discussion regarding Clickhouse plugin.

## Why use ClickHouse for Jaeger?

[ClickHouse](https://github.com/clickhouse/clickhouse) is an analytical column-oriented database management system. It is designed to analyze streams of clicks which are kind of resemblant to spans. It's open-source, optimized for performance, and actively developed.

## How it works?

Jaeger spans are stored in 2 tables. First one contains whole span encoded either in JSON or Protobuf. Second stores key information about spans for searching. This table is indexed by span duration and tags. Also, info about operations is stored in the materialized view. There are not indexes for archived spans. Storing data in replicated local tables with distributed global tables is natively supported. Spans are bufferized. Span buffers are flushed to DB either by timer or after reaching max batch size. Timer interval and batch size can be set in [config file](https://github.com/EinKrebs/jaeger-clickhouse/blob/main/config.yaml).

![Picture of tables](https://github.com/EinKrebs/jaeger-clickhouse/blob/pics/pics/tables.png?raw=true)

## Benchmarks

10^9 traces were flushed using [jaeger-tracegen](https://www.jaegertracing.io/docs/1.25/tools/) to Clickhouse and ElasticSearch servers. Clickhouse server consisted of 3 shards, 2 hosts in each. Elasticsearch server consisted of 6 hosts. All hosts are equal.

#### Clickhouse

Disk usage is 14.2 GiB peak, 8.9 GiB after merging data. At last, 89 * 10^6 spans were recorded.
![ClickHouse stats](https://github.com/EinKrebs/jaeger-clickhouse/blob/pics/pics/clickhouse-disk-usage.png?raw=true)
```sql
SELECT count()
FROM jaeger_index 

┌──count()─┐
│ 81788392 │
└──────────┘
```

#### Elastic

Average disk usage is 17 GiB peak, 13 GiB finally. At last, 23.7 * 10^6 spans were recorded.
![Elasticsearch stats](https://raw.githubusercontent.com/EinKrebs/jaeger-clickhouse/pics/pics/elastic-disk-usage.png)
![Elasticsearch hits](https://raw.githubusercontent.com/EinKrebs/jaeger-clickhouse/pics/pics/elastic-hits.png)

# How to start using Jaeger over ClickHouse

## Documentation

Refer to the [config.yaml](./config.yaml) for all supported configuration options.

* [Kubernetes deployment](./guide-kubernetes.md)
* [Sharding and replication](./guide-sharding-and-replication.md)
* [Multi-tenancy](./guide-multitenancy.md)

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

# Project status

Jaeger ClickHouse is a community-driven project, we would love to hear your feature requests.
Pull requests also will be greatly appreciated.

## Credits

This project is based on https://github.com/bobrik/jaeger/tree/ivan/clickhouse/plugin/storage/clickhouse.