CREATE MATERIALIZED VIEW IF NOT EXISTS %s
ENGINE SummingMergeTree
%s
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
    toDate(timestamp) AS date,
    service,
    operation,
    count() as count
FROM %s -- Here goes local jaeger index table's name
GROUP BY date, service, operation
