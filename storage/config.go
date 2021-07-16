package storage

import "time"

type EncodingType string

const (
	JSONEncoding     EncodingType = "json"
	ProtobufEncoding EncodingType = "protobuf"
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
}
