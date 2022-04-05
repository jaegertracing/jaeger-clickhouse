CREATE TABLE IF NOT EXISTS {{.Table}}
    ON CLUSTER '{cluster}' AS {{.Database}}.{{.Table}}_local
    ENGINE = Distributed('{cluster}', {{.Database}}, {{.Table}}_local, {{.Hash}})
