CREATE TABLE IF NOT EXISTS jaeger_archive_spans_local (
    timestamp DateTime CODEC(Delta, ZSTD(1)),
    traceID String CODEC(ZSTD(1)),
    model String CODEC(ZSTD(3))
) ENGINE MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY traceID
SETTINGS index_granularity=1024
