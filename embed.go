package jaegerclickhouse

import "embed"

//go:embed sqlscripts/local/*
var EmbeddedFilesNoReplication embed.FS

//go:embed sqlscripts/replication/*
var EmbeddedFilesReplication embed.FS
