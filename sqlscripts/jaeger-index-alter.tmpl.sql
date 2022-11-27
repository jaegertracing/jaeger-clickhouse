ALTER TABLE {{.SpansIndexTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
MODIFY {{.TTLTimestamp}}