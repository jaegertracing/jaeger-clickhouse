ALTER TABLE {{.SpansArchiveTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
MODIFY {{.TTLTimestamp}}