package jaegerclickhouse

import "embed"

//go:embed sqlscripts/*
var SQLScripts embed.FS
