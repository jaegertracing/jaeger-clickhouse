CREATE MATERIALIZED VIEW IF NOT EXISTS {{.OperationsTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
    ENGINE {{if .Replication}}ReplicatedSummingMergeTree{{else}}SummingMergeTree{{end}}
    {{.TTLDate}}
    PARTITION BY (
        {{if .Multitenant -}}
        tenant,
        {{- end -}}
        toYYYYMM(date)
    )
    ORDER BY (
        {{if .Multitenant -}}
        tenant,
        {{- end -}}
        date,
        service,
        operation
    )
    SETTINGS index_granularity = 32
    POPULATE
AS SELECT
    {{if .Multitenant -}}
    tenant,
    {{- end -}}
    toDate(timestamp) AS date,
    service,
    operation,
    count() AS count,
    if(
        has(tags.key, 'span.kind'),
        tags.value[indexOf(tags.key, 'span.kind')],
        'unspecified'
    ) AS spankind
FROM {{.Database}}.{{.SpansIndexTable}}
GROUP BY
    {{if .Multitenant -}}
    tenant,
    {{- end -}}
    date,
    service,
    operation,
    tags.key,
    tags.value
