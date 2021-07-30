CREATE TABLE IF NOT EXISTS %s -- spans table
    ON CLUSTER '{cluster}' AS %s -- local spans table
    ENGINE = Distributed('{cluster}', jaeger, %s, cityHash64(traceID)); -- local spans table
CREATE TABLE IF NOT EXISTS jaeger_index -- index table
    ON CLUSTER '{cluster}' AS %s -- local index table
    ENGINE = Distributed('{cluster}', jaeger, %s, cityHash64(traceID)); -- local index table
CREATE TABLE IF NOT EXISTS %s -- operations table
    ON CLUSTER '{cluster}' AS %s -- local operations table
    ENGINE = Distributed('{cluster}', jaeger, %s, rand()); -- local operations table
CREATE TABLE IF NOT EXISTS %s -- spans archive table
    ON CLUSTER '{cluster}' AS %s -- local spans archive table
    ENGINE = Distributed('{cluster}', jaeger, %s, cityHash64(traceID)) -- local spans archive table