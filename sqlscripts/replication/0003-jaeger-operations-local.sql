CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER '{cluster}'
        ENGINE ReplicatedMergeTree
            PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
            SETTINGS index_granularity=32
        POPULATE
AS SELECT toDate(timestamp) AS date,
          service,
          operation,
          count()           as count
   FROM %s -- here goes local index table
   GROUP BY date, service, operation;