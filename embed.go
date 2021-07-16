package jaegerclickhouse

import "embed"

//go:embed sqlscripts/*
var EmbeddedFiles embed.FS
