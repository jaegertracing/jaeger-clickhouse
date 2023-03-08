CREATE TABLE IF NOT EXISTS {{.SpansTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
(
    {{if .Multitenant -}}
    tenant    LowCardinality(String) CODEC (ZSTD(1)),
    {{- end -}}
    timestamp DateTime64(3, 'UTC') CODEC (Delta, ZSTD(1)),
    traceID   String CODEC (ZSTD(1)),
    model     String CODEC (ZSTD(3))
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    {{.TTLTimestamp}}
    PARTITION BY (
        {{if .Multitenant -}}
        tenant,
        {{- end -}}
        toDate(timestamp)
    )
    ORDER BY traceID
    SETTINGS index_granularity = 1024
