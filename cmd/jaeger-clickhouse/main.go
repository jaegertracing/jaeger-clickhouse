package main

import (
	"os"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"

	"github.com/pavolloffay/jaeger-clickhouse/storage"
)

func main() {
	logger := hclog.New(&hclog.LoggerOptions{
		Name:  "jaeger-clickhouse",
		Level: hclog.Warn, // Jaeger only captures >= Warn, so don't bother logging below Warn
	})

	var store shared.PluginServices
	s, err := storage.NewStore(logger)
	if err != nil {
		logger.Error("Failed to crate storage", err)
		os.Exit(1)
	}
	store.Store = s

	grpc.Serve(&store)
	if err = s.Close(); err != nil {
		logger.Error("Failed to close store", "error", err)
		os.Exit(1)
	}
}
