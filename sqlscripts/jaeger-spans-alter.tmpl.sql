ALTER TABLE {{.SpansTable}}
{{if .Replication}}ON CLUSTER '{cluster}'{{end}}
MODIFY {{.TTLTimestamp}}