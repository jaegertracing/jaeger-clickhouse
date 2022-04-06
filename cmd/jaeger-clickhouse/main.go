package main

import (
	"flag"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"

	// Package contains time zone info for connecting to ClickHouse servers with non-UTC time zone
	_ "time/tzdata"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	yaml "gopkg.in/yaml.v3"

	"github.com/jaegertracing/jaeger-clickhouse/storage"
)

func main() {
	var configPath string
	flag.StringVar(&configPath, "config", "", "The absolute path to the ClickHouse plugin's configuration file")
	flag.Parse()

	logger := hclog.New(&hclog.LoggerOptions{
		Name: "jaeger-clickhouse",
		// If this is set to e.g. Warn, the debug logs are never sent to Jaeger even despite
		// --grpc-storage-plugin.log-level=debug
		Level:      hclog.Trace,
		JSONFormat: true,
	})

	cfgFile, err := ioutil.ReadFile(filepath.Clean(configPath))
	if err != nil {
		logger.Error("Could not read config file", "config", configPath, "error", err)
		os.Exit(1)
	}
	var cfg storage.Configuration
	err = yaml.Unmarshal(cfgFile, &cfg)
	if err != nil {
		logger.Error("Could not parse config file", "error", err)
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		err = http.ListenAndServe(cfg.MetricsEndpoint, nil)
		if err != nil {
			logger.Error("Failed to listen for metrics endpoint", "error", err)
		}
	}()

	var pluginServices shared.PluginServices
	store, err := storage.NewStore(logger, cfg)
	if err != nil {
		logger.Error("Failed to create a storage", err)
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
