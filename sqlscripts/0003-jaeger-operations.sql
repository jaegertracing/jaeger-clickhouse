CREATE MATERIALIZED VIEW IF NOT EXISTS %s
ENGINE SummingMergeTree
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
    toDate(timestamp) AS date,
    service,
    operation,
    count() as count
FROM %s
GROUP BY date, service, operation
