package storage

import "time"

type EncodingType string

const (
	JSONEncoding           EncodingType = "json"
	ProtobufEncoding       EncodingType = "protobuf"
	defaultBatchSize                    = 10_000
	defaultBatchDelay                   = time.Second * 5
	defaultUsername                     = "default"
	defaultDatabaseName                 = "default"
	defaultMetricsEndpoint              = "localhost:9090"

	defaultSpansTable      = "jaeger_spans_local"
	defaultSpansIndexTable = "jaeger_index_local"
	defaultOperationsTable = "jaeger_operations_local"
)

type Configuration struct {
	// Batch write size. Default is 10_000.
	BatchWriteSize int64 `yaml:"batch_write_size"`
	// Batch flush interval. Default is 5s.
	BatchFlushInterval time.Duration `yaml:"batch_flush_interval"`
	// Encoding either json or protobuf. Default is json.
	Encoding EncodingType `yaml:"encoding"`
	// ClickHouse address e.g. tcp://localhost:9000.
	Address string `yaml:"address"`
	// Directory with .sql files that are run at plugin startup.
	InitSQLScriptsDir string `yaml:"init_sql_scripts_dir"`
	// Indicates whether to use TLS
	TLSConnection bool `yaml:"tls_connection"`
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
	// Table with spans. Default "jaeger_spans_local".
	SpansTable string `yaml:"spans_table"`
	// Span index table. Default "jaeger_index_local".
	SpansIndexTable string `yaml:"spans_index_table"`
	// Operations table. Default "jaeger_operations_local.
	OperationsTable string `yaml:"operations_table"`
}

func (cfg *Configuration) setDefaults() {
	if cfg.BatchWriteSize == 0 {
		cfg.BatchWriteSize = defaultBatchSize
	}
	if cfg.BatchFlushInterval == 0 {
		cfg.BatchFlushInterval = defaultBatchDelay
	}
	if cfg.Encoding == "" {
		cfg.Encoding = JSONEncoding
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
	if cfg.SpansTable == "" {
		cfg.SpansTable = defaultSpansTable
	}
	if cfg.SpansIndexTable == "" {
		cfg.SpansIndexTable = defaultSpansIndexTable
	}
	if cfg.OperationsTable == "" {
		cfg.OperationsTable = defaultOperationsTable
	}
}
