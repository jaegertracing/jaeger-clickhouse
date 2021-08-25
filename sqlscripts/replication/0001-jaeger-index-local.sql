CREATE TABLE IF NOT EXISTS %s ON CLUSTER '{cluster}'
(
    timestamp  DateTime CODEC (Delta, ZSTD(1)),
    traceID    String CODEC (ZSTD(1)),
    service    LowCardinality(String) CODEC (ZSTD(1)),
    operation  LowCardinality(String) CODEC (ZSTD(1)),
    durationUs UInt64 CODEC (ZSTD(1)),
    tags Nested
    (
        key LowCardinality(String),
        value String
    ) CODEC(ZSTD(1)),
    INDEX idx_tag_keys tags.key TYPE bloom_filter(0.01) GRANULARITY 64,
    INDEX idx_duration durationUs TYPE minmax GRANULARITY 1
) ENGINE ReplicatedMergeTree
      %s
      PARTITION BY toDate(timestamp)
      ORDER BY (service, -toUnixTimestamp(timestamp))
      SETTINGS index_granularity = 1024;