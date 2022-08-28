# Kubernetes Deployment

This is a guide to deploy Jaeger with Clickhouse storage on Kubernetes.

## Prerequisites

1. Deploy [Jaeger operator](https://github.com/jaegertracing/jaeger-operator). Note that `grpc-plugin` storage type is supported since version 1.25.0.
2. Deploy [Clickhouse operator](https://github.com/Altinity/clickhouse-operator)
3. Deploy [Zookeeper](https://github.com/Altinity/clickhouse-operator/blob/master/docs/replication_setup.md) (if replication is used)

## Deploy

Deploy Clickhouse:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: clickhouse.altinity.com/v1
kind: ClickHouseInstallation
metadata:
  name: jaeger
  labels:
    jaeger-clickhouse: demo
spec:
  configuration:
    clusters:
      - name: cluster1
        layout:
          shardsCount: 1
EOF
```

Create config map for Jaeger Clickhouse plugin:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: ConfigMap
metadata:
  name: jaeger-clickhouse
  labels:
    jaeger-clickhouse: demo
data:
  config.yaml: |
    address: clickhouse-jaeger:9000
    username: clickhouse_operator
    password: clickhouse_operator_password
    spans_table:
    spans_index_table:
    operations_table:
EOF
```

Deploy Jaeger:

```yaml
cat <<EOF | kubectl apply -f -
apiVersion: jaegertracing.io/v1
kind: Jaeger
metadata:
  name: jaeger-clickhouse
  labels:
    jaeger-clickhouse: demo
spec:
  storage:
    type: grpc-plugin
    grpcPlugin:
      image: ghcr.io/jaegertracing/jaeger-clickhouse:0.7.0
    options:
      grpc-storage-plugin:
        binary: /plugin/jaeger-clickhouse
        configuration-file: /plugin-config/config.yaml
        log-level: debug
  volumeMounts:
    - name: plugin-config
      mountPath: /plugin-config
  volumes:
    - name: plugin-config
      configMap:
        name: jaeger-clickhouse
EOF
```

## Delete all

```bash
kubectl delete jaeger,cm,chi -l jaeger-clickhouse=demo
```
