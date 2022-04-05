CREATE TABLE IF NOT EXISTS {{.SpansTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
(
    timestamp DateTime CODEC (Delta, ZSTD(1)),
    traceID   String CODEC (ZSTD(1)),
    model     String CODEC (ZSTD(3))
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    {{.TTLTimestamp}}
    PARTITION BY (
        toDate(timestamp)
    )
    ORDER BY traceID
    SETTINGS index_granularity = 1024
