CREATE MATERIALIZED VIEW IF NOT EXISTS jaeger_operations_local
ENGINE SummingMergeTree
PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
SETTINGS index_granularity=32
POPULATE
AS SELECT
    toDate(timestamp) AS date,
    service,
    operation,
    count() as count,
    if(has(tags.key, 'span.kind'), tags.value[indexOf(tags.key, 'span.kind')], '') as spankind
FROM jaeger_index_local
GROUP BY date, service, operation, tags.key, tags.value
