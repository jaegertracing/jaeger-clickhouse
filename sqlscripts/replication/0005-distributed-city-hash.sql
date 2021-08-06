CREATE TABLE IF NOT EXISTS %s -- global table name
    ON CLUSTER '{cluster}' AS %s -- local table name
    ENGINE = Distributed('{cluster}', %s, %s, cityHash64(traceID)); -- local table name