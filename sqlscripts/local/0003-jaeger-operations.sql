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
    count() as count,
    if(has(tags.key, 'span.kind'), tags.value[indexOf(tags.key, 'span.kind')], '') as spankind
FROM %s -- Here goes local jaeger index table's name
GROUP BY date, service, operation, tags.key, tags.value
