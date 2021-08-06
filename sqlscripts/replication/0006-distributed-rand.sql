CREATE TABLE IF NOT EXISTS %s -- operations table
    ON CLUSTER '{cluster}' AS %s -- local operations table
    ENGINE = Distributed('{cluster}', %s, %s, rand()); -- local operations table