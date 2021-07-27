CREATE MATERIALIZED VIEW IF NOT EXISTS jaeger_operations_local
ENGINE SummingMergeTree
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
