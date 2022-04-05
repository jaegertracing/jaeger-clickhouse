CREATE TABLE IF NOT EXISTS {{.SpansIndexTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
(
    {{if .Multitenant -}}
    tenant     LowCardinality(String) CODEC (ZSTD(1)),
    {{- end -}}
    timestamp  DateTime CODEC (Delta, ZSTD(1)),
    traceID    String CODEC (ZSTD(1)),
    service    LowCardinality(String) CODEC (ZSTD(1)),
    operation  LowCardinality(String) CODEC (ZSTD(1)),
    durationUs UInt64 CODEC (ZSTD(1)),
    tags Nested
    (
        key LowCardinality(String),
        value String
    ) CODEC (ZSTD(1)),
    INDEX idx_tag_keys tags.key TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_duration durationUs TYPE minmax GRANULARITY 1
) ENGINE {{if .Replication}}ReplicatedMergeTree{{else}}MergeTree(){{end}}
    {{.TTLTimestamp}}
    PARTITION BY (
        {{if .Multitenant -}}
        tenant,
        {{- end -}}
        toDate(timestamp)
    )
    ORDER BY (service, -toUnixTimestamp(timestamp))
    SETTINGS index_granularity = 1024
