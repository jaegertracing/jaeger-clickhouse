CREATE MATERIALIZED VIEW IF NOT EXISTS %s ON CLUSTER '{cluster}'
        ENGINE ReplicatedMergeTree
            %s
            PARTITION BY toYYYYMM(date) ORDER BY (date, service, operation)
            SETTINGS index_granularity=32
        POPULATE
AS SELECT toDate(timestamp)                                                              AS date,
          service,
          operation,
          count()                                                                        as count,
          if(has(tag_keys, 'span.kind'), tag_values[indexOf(tag_keys, 'span.kind')], '') as spankind
   FROM %s -- here goes local index table
   GROUP BY date, service, operation, tag_keys, tag_values;