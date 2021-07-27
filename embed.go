package jaegerclickhouse

import "embed"

//go:embed sqlscripts/no_replication/*
var EmbeddedFilesNoReplication embed.FS

////go:embed sqlscripts/replication/*
//var EmbeddedFilesReplication embed.FS
