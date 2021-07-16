package main

import (
	"flag"
	"io/ioutil"
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"gopkg.in/yaml.v3"

	jaegerclickhouse "github.com/pavolloffay/jaeger-clickhouse"
	"github.com/pavolloffay/jaeger-clickhouse/storage"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "The absolute path to the ClickHouse plugin's configuration file")
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name:  "jaeger-clickhouse",
		Level: hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
	})

	cfgFile, err := ioutil.ReadFile(configPath)
	if err != nil {
		logger.Error("Could not read config file: %q: %q", configPath, err)
		os.Exit(1)
	}
	var cfg storage.Configuration
	err = yaml.Unmarshal(cfgFile, &cfg)
	if err != nil {
		logger.Error("Could not parse config file: %q", err)
	}

	var pluginServices shared.PluginServices
	store, err := storage.NewStore(logger, cfg, jaegerclickhouse.EmbeddedFiles)
	if err != nil {
		logger.Error("Failed to crate storage", err)
		os.Exit(1)
	}
	pluginServices.Store = store
	pluginServices.ArchiveStore = store

	grpc.Serve(&pluginServices)
	if err = store.Close(); err != nil {
		logger.Error("Failed to close store", "error", err)
		os.Exit(1)
	}
}
