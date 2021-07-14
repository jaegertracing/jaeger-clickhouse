GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)

TOOLS_MOD_DIR = ./internal/tools

JAEGER_ALL_IN_ONE ?= ${HOME}/projects/jaegertracing/jaeger/cmd/all-in-one/all-in-one-linux-amd64

.PHONY: build
build:
	go build -o jaeger-clickhouse-$(GOOS)-$(GOARCH) ./cmd/jaeger-clickhouse/main.go

.PHONY: run
run:
	SPAN_STORAGE_TYPE=grpc-plugin ${JAEGER_ALL_IN_ONE} --grpc-storage-plugin.binary=./jaeger-clickhouse-$(GOOS)-$(GOARCH) --grpc-storage-plugin.configuration-file=./config.yaml

.PHONY: fmt
fmt:
	go fmt ./...
	goimports -w  -local github.com/pavolloffay/jaeger-clickhouse ./

.PHONY: install-tools
install-tools:
	cd $(TOOLS_MOD_DIR) && go install golang.org/x/tools/cmd/goimports

