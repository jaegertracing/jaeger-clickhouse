CREATE TABLE IF NOT EXISTS {{.SpansArchiveTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
(
    {{if .Multitenant -}}
    tenant    LowCardinality(String) CODEC (ZSTD(1)),
    {{- end -}}
    timestamp DateTime CODEC (Delta, ZSTD(1)),
    traceID   String CODEC (ZSTD(1)),
    model     String CODEC (ZSTD(3))
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    {{.TTLTimestamp}}
    PARTITION BY (
        {{if .Multitenant -}}
        tenant,
        {{- end -}}
        toYYYYMM(timestamp)
    )
    ORDER BY traceID
    SETTINGS index_granularity = 1024
