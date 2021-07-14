package storage

import "time"

type Configuration struct {
	// ClickHouse address e.g. tcp://localhost:9000.
	Address string `yaml:"address"`
	// Directory with .sql files that are run at plugin startup.
	InitSQLScriptsDir string `yaml:"init_sql_scripts_dir"`
	// Batch write size. Default is 10_000.
	Size int `yaml:"size"`
	// Write delay. Default is 5s.
	Delay time.Duration `yaml:"delay"`
}
