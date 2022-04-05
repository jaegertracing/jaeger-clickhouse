CREATE MATERIALIZED VIEW IF NOT EXISTS {{.OperationsTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
    ENGINE {{if .Replication}}ReplicatedSummingMergeTree{{else}}SummingMergeTree{{end}}
    {{.TTLDate}}
    PARTITION BY (
        toYYYYMM(date)
    )
    ORDER BY (
        date,
        service,
        operation
    )
    SETTINGS index_granularity = 32
    POPULATE
AS SELECT
    toDate(timestamp) AS date,
    service,
    operation,
    count() AS count,
    if(
        has(tags.key, 'span.kind'),
        tags.value[indexOf(tags.key, 'span.kind')],
        ''
    ) AS spankind
FROM {{.Database}}.{{.SpansIndexTable}}
GROUP BY
    date,
    service,
    operation,
    tags.key,
    tags.value
