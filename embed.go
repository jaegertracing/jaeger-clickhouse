package jaeger_clickhouse

import "embed"

//go:embed sqlscripts/*
var EmbeddedFiles embed.FS
