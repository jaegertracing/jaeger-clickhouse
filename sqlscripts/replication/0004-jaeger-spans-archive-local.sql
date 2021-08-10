CREATE TABLE IF NOT EXISTS %s ON CLUSTER '{cluster}'
(
    timestamp DateTime CODEC (Delta, ZSTD(1)),
    traceID   String CODEC (ZSTD(1)),
    model     String CODEC (ZSTD(3))
) ENGINE ReplicatedMergeTree
      %s
      PARTITION BY toYYYYMM(timestamp)
      ORDER BY traceID
      SETTINGS index_granularity = 1024
