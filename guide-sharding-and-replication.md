# Sharding and Replication

This is a guide how to setup sharding and replication for Jaeger data.
This guide uses [clickhouse-operator](https://github.com/Altinity/clickhouse-operator) to deploy
the storage.

Note that the Jaeger ClickHouse plugin supports creating replicated schema out-of-the-box. Therefore,
this guide is not necessary for setting up default replicated deployment. Also note that the
ClickHouse operator uses by default `Ordinary` database engine, which does not work with the
embedded replication scripts in Jaeger.
Refer to the `config.yaml` how to setup replicated deployment.

## Sharding

Sharding is a feature that allows splitting the data into multiple Clickhouse nodes to
increase throughput and decrease latency.
The sharding feature uses `Distributed` engine that is backed by local tables.
The distributed engine is a "virtual" table that does not store any data. It is used as
an interface to insert and query data.

To setup sharding run the following statements on all nodes in the cluster.
The "local" tables have to be created on the nodes before the distributed table.

```sql
CREATE DATABASE jaeger ENGINE=Atomic;
USE jaeger;

CREATE TABLE IF NOT EXISTS jaeger_spans AS jaeger_spans_local ENGINE = Distributed('{cluster}', default, jaeger_spans_local, cityHash64(traceID));
CREATE TABLE IF NOT EXISTS jaeger_index AS jaeger_index_local ENGINE = Distributed('{cluster}', default, jaeger_index_local, cityHash64(traceID));
CREATE TABLE IF NOT EXISTS jaeger_operations AS jaeger_operations_local ENGINE = Distributed('{cluster}', default, jaeger_operations_local, rand());
```

* The `AS <table-name>` statement creates table with the same schema as the specified one.
* The `Distributed` engine takes as parameters cluster , database, table name and sharding key.

If the distributed table is not created on all Clickhouse nodes the Jaeger query fails to get the data from the storage.

### Deploy Clickhouse

Deploy Clickhouse with 2 shards:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: jaeger
spec:
  configuration:
    clusters:
      - name: cluster1
        layout:
          shardsCount: 2
EOF
```

Use the following command to run `clickhouse-client` on Clickhouse nodes and create the distributed tables:
```bash
kubectl exec -it statefulset.apps/chi-jaeger-cluster1-0-0 -- clickhouse-client
```

### Plugin configuration

The plugin has to be configured to write and read that from the global tables:

```yaml
address: clickhouse-jaeger:9000
# database: jaeger
spans_table: jaeger_spans
spans_index_table: jaeger_index
operations_table: jaeger_operations
```

## Replication

Replication as the name suggest automatically replicates the data across multiple Clickhouse nodes.
It is used to accomplish high availability, load scaling and migration/updates.

The replication uses Zookeeper. Refer to the Clickhouse operator how to deploy Zookeeper.

Zookeeper allows us to use `ON CLUSTER` to automatically replicate table creation on all nodes.
Therefore the following command can be run only on a single Clickhouse node:

```sql
CREATE DATABASE IF NOT EXISTS jaeger ON CLUSTER '{cluster}' ENGINE=Atomic;
USE jaeger;

CREATE TABLE IF NOT EXISTS jaeger_spans_local ON CLUSTER '{cluster}' (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    traceID String CODEC(ZSTD(1)),
    model String CODEC(ZSTD(3))
) ENGINE ReplicatedMergeTree
PARTITION BY toDate(timestamp)
ORDER BY traceID
SETTINGS index_granularity=1024;

CREATE TABLE IF NOT EXISTS jaeger_index_local ON CLUSTER '{cluster}' (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    traceID String CODEC(ZSTD(1)),
    service LowCardinality(String) CODEC(ZSTD(1)),
    operation LowCardinality(String) CODEC(ZSTD(1)),
    durationUs UInt64 CODEC(ZSTD(1)),
    tags Array(String) CODEC(ZSTD(1)),
    INDEX idx_tags tags TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_duration durationUs TYPE minmax GRANULARITY 1
) ENGINE ReplicatedMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service, -toUnixTimestamp(timestamp))
SETTINGS index_granularity=1024;

CREATE MATERIALIZED VIEW IF NOT EXISTS jaeger_operations_local ON CLUSTER '{cluster}'
ENGINE ReplicatedMergeTree
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
    toDate(timestamp) AS date,
    service,
    operation,
count() as count
FROM jaeger.jaeger_index_local
GROUP BY date, service, operation;


CREATE TABLE IF NOT EXISTS jaeger_spans ON CLUSTER '{cluster}' AS jaeger.jaeger_spans_local ENGINE = Distributed('{cluster}', jaeger, jaeger_spans_local, cityHash64(traceID));
CREATE TABLE IF NOT EXISTS jaeger_index ON CLUSTER '{cluster}' AS jaeger.jaeger_index_local ENGINE = Distributed('{cluster}', jaeger, jaeger_index_local, cityHash64(traceID));
CREATE TABLE IF NOT EXISTS jaeger_operations on CLUSTER '{cluster}' AS jaeger.jaeger_operations_local ENGINE = Distributed('{cluster}', jaeger, jaeger_operations_local, rand());
```

### Deploy Clickhouse

Before deploying Clickhouse make sure Zookeeper is running in `zoo1ns` namespace.

Deploy Clickhouse with 3 shards and 2 replicas. In total Clickhouse operator will deploy 6 pods:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: jaeger
spec:
  defaults:
    templates:
      dataVolumeClaimTemplate: data-volume-template
      logVolumeClaimTemplate: log-volume-template
  configuration:
    zookeeper:
      nodes:
        - host: zookeeper.zoo1ns
    clusters:
      - name: cluster1
        layout:
          shardsCount: 3
          replicasCount: 2
  templates:
    volumeClaimTemplates:
      - name: data-volume-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 1Gi
      - name: log-volume-template
        spec:
          accessModes:
            - ReadWriteOnce
          resources:
            requests:
              storage: 100Mi
EOF
```

The Clickhouse deployment will look like this:
```bash
k get statefulsets
NAME                      READY   AGE
chi-jaeger-cluster1-0-0   1/1     17m    # shard 0
chi-jaeger-cluster1-0-1   1/1     17m    # shard 0, replica 1
chi-jaeger-cluster1-1-0   1/1     16m    # shard 1
chi-jaeger-cluster1-1-1   1/1     16m    # shard 1, replica 1
chi-jaeger-cluster1-2-0   1/1     7m43s  # shard 2
chi-jaeger-cluster1-2-1   1/1     7m26s  # shard 2, replica 1
```

#### Scaling up

Just increase `shardsCount` number and new Clickhouse node will come up. It will have initialized Jaeger tables so
no other steps are required. Note that the old data are not re-balanced, only new writes take into the account
the new node.

## Useful Commands

### SQL

```sql
show tables;
select count() from jaeger_spans;
```

### Kubectl

```bash
kubectl get chi -o wide
kubectl port-forward service/clickhouse-jaeger 9000:9000
kubectl delete chi jaeger
```
