package storage

import (
	"time"

	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousespanstore"
)

type EncodingType string

const (
	defaultEncoding                     = JSONEncoding
	JSONEncoding           EncodingType = "json"
	ProtobufEncoding       EncodingType = "protobuf"
	defaultMaxSpanCount                 = int(1e7)
	defaultBatchSize                    = 10_000
	defaultBatchDelay                   = time.Second * 5
	defaultUsername                     = "default"
	defaultDatabaseName                 = "default"
	defaultMetricsEndpoint              = "localhost:9090"
	defaultMaxNumSpans                  = 0

	defaultSpansTable      clickhousespanstore.TableName = "jaeger_spans"
	defaultSpansIndexTable clickhousespanstore.TableName = "jaeger_index"
	defaultOperationsTable clickhousespanstore.TableName = "jaeger_operations"
)

type Configuration struct {
	// Batch write size. Default is 10_000.
	BatchWriteSize int64 `yaml:"batch_write_size"`
	// Batch flush interval. Default is 5s.
	BatchFlushInterval time.Duration `yaml:"batch_flush_interval"`
	// Maximal amount of spans that can be pending writes at a time.
	// New spans exceeding this limit will be discarded,
	// keeping memory in check if there are issues writing to ClickHouse.
	// Check the "jaeger_clickhouse_discarded_spans" metric to keep track of discards.
	// Default 10_000_000, or disable the limit entirely by setting to 0.
	MaxSpanCount int `yaml:"max_span_count"`
	// Encoding either json or protobuf. Default is json.
	Encoding EncodingType `yaml:"encoding"`
	// ClickHouse address e.g. localhost:9000.
	Address string `yaml:"address"`
	// Directory with .sql files to run at plugin startup, mainly for integration tests.
	// Depending on the value of init_tables, this can be run as a
	// replacement or supplement to creating default tables for span storage.
	// If init_tables is also enabled, the scripts in this directory will be run first.
	InitSQLScriptsDir string `yaml:"init_sql_scripts_dir"`
	// Whether to automatically attempt to create tables in ClickHouse.
	// By default, this is enabled if init_sql_scripts_dir is empty,
	// or disabled if init_sql_scripts_dir is provided.
	InitTables *bool `yaml:"init_tables"`
	// Indicates location of TLS certificate used to connect to database.
	CaFile string `yaml:"ca_file"`
	// Username for connection to database. Default is "default".
	Username string `yaml:"username"`
	// Password for connection to database.
	Password string `yaml:"password"`
	// Database name. Default is "default"
	Database string `yaml:"database"`
	// Endpoint for scraping prometheus metrics e.g. localhost:9090.
	MetricsEndpoint string `yaml:"metrics_endpoint"`
	// Whether to use SQL scripts supporting replication and sharding. Default false.
	Replication bool `yaml:"replication"`
	// If non-empty, enables multitenancy in SQL scripts, and assigns the tenant name for this instance.
	Tenant string `yaml:"tenant"`
	// Table with spans. Default "jaeger_spans_local" or "jaeger_spans" when replication is enabled.
	SpansTable clickhousespanstore.TableName `yaml:"spans_table"`
	// Span index table. Default "jaeger_index_local" or "jaeger_index" when replication is enabled.
	SpansIndexTable clickhousespanstore.TableName `yaml:"spans_index_table"`
	// Operations table. Default "jaeger_operations_local" or "jaeger_operations" when replication is enabled.
	OperationsTable   clickhousespanstore.TableName `yaml:"operations_table"`
	spansArchiveTable clickhousespanstore.TableName
	// TTL for data in tables in days. If 0, no TTL is set. Default 0.
	TTLDays uint `yaml:"ttl"`
	// The maximum number of spans to fetch per trace. If 0, no limits is set. Default 0.
	MaxNumSpans uint `yaml:"max_num_spans"`
}

func (cfg *Configuration) setDefaults() {
	if cfg.BatchWriteSize == 0 {
		cfg.BatchWriteSize = defaultBatchSize
	}
	if cfg.BatchFlushInterval == 0 {
		cfg.BatchFlushInterval = defaultBatchDelay
	}
	if cfg.MaxSpanCount == 0 {
		cfg.MaxSpanCount = defaultMaxSpanCount
	}
	if cfg.Encoding == "" {
		cfg.Encoding = defaultEncoding
	}
	if cfg.InitTables == nil {
		// Decide whether to init tables based on whether a custom script path was provided
		var defaultInitTables bool
		if cfg.InitSQLScriptsDir == "" {
			defaultInitTables = true
		} else {
			defaultInitTables = false
		}
		cfg.InitTables = &defaultInitTables
	}
	if cfg.Username == "" {
		cfg.Username = defaultUsername
	}
	if cfg.Database == "" {
		cfg.Database = defaultDatabaseName
	}
	if cfg.MetricsEndpoint == "" {
		cfg.MetricsEndpoint = defaultMetricsEndpoint
	}
	if cfg.MaxNumSpans == 0 {
		cfg.MaxNumSpans = defaultMaxNumSpans
	}
	if cfg.SpansTable == "" {
		if cfg.Replication {
			cfg.SpansTable = defaultSpansTable
			cfg.spansArchiveTable = defaultSpansTable + "_archive"
		} else {
			cfg.SpansTable = defaultSpansTable.ToLocal()
			cfg.spansArchiveTable = (defaultSpansTable + "_archive").ToLocal()
		}
	} else {
		cfg.spansArchiveTable = cfg.SpansTable + "_archive"
	}
	if cfg.SpansIndexTable == "" {
		if cfg.Replication {
			cfg.SpansIndexTable = defaultSpansIndexTable
		} else {
			cfg.SpansIndexTable = defaultSpansIndexTable.ToLocal()
		}
	}
	if cfg.OperationsTable == "" {
		if cfg.Replication {
			cfg.OperationsTable = defaultOperationsTable
		} else {
			cfg.OperationsTable = defaultOperationsTable.ToLocal()
		}
	}
}

func (cfg *Configuration) GetSpansArchiveTable() clickhousespanstore.TableName {
	return cfg.spansArchiveTable
}
