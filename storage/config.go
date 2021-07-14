package storage

import "time"

type Configuration struct {
	Address       string `yaml:"address"`
	SQLScriptsDir string `yaml:"sql_scripts_dir"`
	// Batch write size. Default is 10_000.
	Size int `yaml:"size"`
	// Write delay. Default is 5s.
	Delay time.Duration `yaml:"delay"`
}
