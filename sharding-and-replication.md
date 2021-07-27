# Sharding and Replication

This is a guide how to setup sharding and replication for Jaeger data.
This guide uses [clickhouse-operator](https://github.com/Altinity/clickhouse-operator) to deploy
the storage.

## Sharding

Sharding is a feature that allows splitting the data into multiple Clickhouse nodes to
increase throughput and decrease latency.
The sharding feature uses `Distributed` engine that is backed by local tables.
The distributed engine is a "virtual" table that does not store any data. It is used as 
an interface to insert and query data.

To setup sharding run the following statements on all nodes in the `sharded` cluster.
The "local" tables have to be created on the nodes before the distributed table.

```sql
CREATE TABLE IF NOT EXISTS jaeger_spans AS jaeger_spans_local ENGINE = Distributed(sharded, default, jaeger_spans_local, rand());
CREATE TABLE IF NOT EXISTS jaeger_index AS jaeger_index_local ENGINE = Distributed(sharded, default, jaeger_index_local, rand());
CREATE TABLE IF NOT EXISTS jaeger_operations AS jaeger_operations_local ENGINE = Distributed(sharded, default, jaeger_operations_local, rand());
```

The `AS <table-name>` statement creates table with the same schema as the specified one. 
The `Distributed` engine takes as parameters cluster , database, table name and sharding key.

If the distributed table is not created on all ClickHouse nodes the Jaeger query fails to get the data from the storage.

### Deploy Clickhouse

Deploy Clickhouse with 2 shards:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "simple"
spec:
  configuration:
    clusters:
      - name: "sharded"
        layout:
          shardsCount: 2
EOF
```

Use the following command to run `clickhouse-client` on Clickhouse nodes and create the distributed tables:
```bash
kubectl exec -it service/chi-simple-sharded-1-0 -- clickhouse-client
```

### Plugin config

The plugin has to be configured to write and read that from the global tables:

```yaml
address: tcp://clickhouse-simple:9000
spans_table: jaeger_spans
spans_index_table: jaeger_index
operations_table: jaeger_operations
```

## Replication

Replication as the name suggest automatically replicates the data across multiple Clickhouse nodes.
It is used to accomplish high availability, load scaling and migration/updates.

The replication uses Zookeeper. Refer to the Clickhouse operator on how to deploy Zookeeper.

Zookeeper allows us to use `ON CLUSTER` to automatically replicate table creation on all nodes.
So the following command can be run only on a single Clickhouse node:

```sql
CREATE TABLE IF NOT EXISTS jaeger_spans_local ON CLUSTER sharded  (
                                                                timestamp DateTime CODEC(Delta, ZSTD(1)),
    traceID String CODEC(ZSTD(1)),
    model String CODEC(ZSTD(3))
    ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{shard}/jaeger_spans', '{replica}')
    PARTITION BY toDate(timestamp)
    ORDER BY traceID
    SETTINGS index_granularity=1024;

CREATE TABLE IF NOT EXISTS jaeger_index_local ON CLUSTER sharded (
                                                               timestamp DateTime CODEC(Delta, ZSTD(1)),
    traceID String CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    durationUs UInt64 CODEC(ZSTD(1)),
    tags Array(String) CODEC(ZSTD(1)),
    INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_duration durationUs TYPE minmax GRANULARITY 1
    ) ENGINE ReplicatedMergeTree('/clickhouse/tables/{shard}/jaeger_index', '{replica}')
    PARTITION BY toDate(timestamp)
    ORDER BY (service, -toUnixTimestamp(timestamp))
    SETTINGS index_granularity=1024;

CREATE MATERIALIZED VIEW IF NOT EXISTS jaeger_operations_local ON CLUSTER sharded
ENGINE ReplicatedMergeTree('/clickhouse/tables/{shard}/jaeger_operations', '{replica}')
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
              toDate(timestamp) AS date,
  service,
  operation,
  count() as count
   FROM jaeger_index
   GROUP BY date, service, operation


CREATE TABLE IF NOT EXISTS jaeger_spans ON CLUSTER sharded AS jaeger_spans_local ENGINE = Distributed(sharded, default, jaeger_spans_local, rand());
CREATE TABLE IF NOT EXISTS jaeger_index ON CLUSTER sharded AS jaeger_index_local ENGINE = Distributed(sharded, default, jaeger_index_local, rand());
CREATE TABLE IF NOT EXISTS jaeger_operations on CLUSTER sharded AS jaeger_operations_local ENGINE = Distributed(sharded, default, jaeger_operations_local, rand());
```

### Deploy Clickhouse

Zookeeper was to be deployed before deploying Clickhouse.

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: "clickhouse.altinity.com/v1"
kind: "ClickHouseInstallation"
metadata:
  name: "simple"
spec:
  configuration:
    zookeeper:
      nodes:
        - host: zookeeper.zoo1ns
    clusters:
      - name: "sharded"
        layout:
          shardsCount: 2
          replicasCount: 2
  templates:
    podTemplates:
      - name: clickhouse-with-empty-dir-volume-template
        spec:
          containers:
            - name: clickhouse-pod
              image: yandex/clickhouse-server:20.7
              volumeMounts:
                - name: clickhouse-storage
                  mountPath: /var/lib/clickhouse
          volumes:
            - name: clickhouse-storage
              emptyDir:
                medium: "" # accepted values:  empty str (means node's default medium) or "Memory"
                sizeLimit: 1Gi
EOF
```

## Useful queries

```sql
show tables
select count() from jaeger_spans
```
