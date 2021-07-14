GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

JAEGER_ALL_IN_ONE ?= ${HOME}/projects/jaegertracing/jaeger/cmd/all-in-one/all-in-one-linux-amd64

.PHONY: build
build:
	go build -o jaeger-clickhouse-$(GOOS)-$(GOARCH) ./cmd/jaeger-clickhouse/main.go

.PHONY: run
run:
	SPAN_STORAGE_TYPE=grpc-plugin ${JAEGER_ALL_IN_ONE} --grpc-storage-plugin.binary=./jaeger-clickhouse-$(GOOS)-$(GOARCH)
