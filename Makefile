GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOBUILD=CGO_ENABLED=0 installsuffix=cgo go build -trimpath

TOOLS_MOD_DIR = ./internal/tools

.PHONY: build
build:
	${GOBUILD} -o jaeger-clickhouse-$(GOOS)-$(GOARCH) ./cmd/jaeger-clickhouse/main.go

.PHONY: run
run:
	docker run --rm --link some-clickhouse-server -it -u ${shell id -u} -v "${PWD}:/data" -e SPAN_STORAGE_TYPE=grpc-plugin jaegertracing/all-in-one:1.24.0 --grpc-storage-plugin.binary=/data/jaeger-clickhouse-$(GOOS)-$(GOARCH) --grpc-storage-plugin.configuration-file=/data/config.yaml

.PHONY: fmt
fmt:
	go fmt ./...
	goimports -w  -local github.com/pavolloffay/jaeger-clickhouse ./

.PHONY: tar
tar:
	tar -czvf jaeger-clickhouse-$(GOOS)-$(GOARCH).tar.gz  jaeger-clickhouse-$(GOOS)-$(GOARCH) config.yaml

.PHONY: install-tools
install-tools:
	cd $(TOOLS_MOD_DIR) && go install golang.org/x/tools/cmd/goimports

