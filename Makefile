GOOS ?= $(shell go env GOOS)
GOARCH ?= $(shell go env GOARCH)
GOBUILD=CGO_ENABLED=0 installsuffix=cgo go build -trimpath

TOOLS_MOD_DIR = ./internal/tools
JAEGER_VERSION ?= 1.24.0

.PHONY: build
build:
	${GOBUILD} -o jaeger-clickhouse-$(GOOS)-$(GOARCH) ./cmd/jaeger-clickhouse/main.go

.PHONY: run
run:
	docker run --rm --name jaeger -e JAEGER_DISABLED=true --link some-clickhouse-server -it -u ${shell id -u} -p16686:16686 -p14250:14250 -p14268:14268 -p6831:6831/udp -v "${PWD}:/data" -e SPAN_STORAGE_TYPE=grpc-plugin jaegertracing/all-in-one:${JAEGER_VERSION} --query.ui-config=/data/jaeger-ui.json --grpc-storage-plugin.binary=/data/jaeger-clickhouse-$(GOOS)-$(GOARCH) --grpc-storage-plugin.configuration-file=/data/config.yaml --grpc-storage-plugin.log-level=debug

.PHONY: run-tls
run-tls:
	docker run --rm --name jaeger -e JAEGER_DISABLED=true -it -u ${shell id -u} --network="host" -p16686:16686 -p14250:14250 -p14268:14268 -p6831:6831/udp -v "/usr/share/zoneinfo/:/usr/share/zoneinfo" -v "${PWD}:/data" -e SPAN_STORAGE_TYPE=grpc-plugin jaegertracing/all-in-one:${JAEGER_VERSION} --query.ui-config=/data/jaeger-ui.json --grpc-storage-plugin.binary=/data/jaeger-clickhouse-$(GOOS)-$(GOARCH) --grpc-storage-plugin.configuration-file=/data/config.yaml --grpc-storage-plugin.log-level=debug

.PHONY: run-hotrod
run-hotrod:
	docker run --rm --env JAEGER_AGENT_HOST=localhost --network="host" --env JAEGER_AGENT_PORT=6831  -p8080-8083:8080-8083 jaegertracing/example-hotrod:${JAEGER_VERSION} all

.PHONY: fmt
fmt:
	go fmt ./...
	goimports -w  -local github.com/pavolloffay/jaeger-clickhouse ./

.PHONY: lint
lint:
	golangci-lint run --allow-parallel-runners ./...

.PHONY: tar
tar:
	tar -czvf jaeger-clickhouse-$(GOOS)-$(GOARCH).tar.gz  jaeger-clickhouse-$(GOOS)-$(GOARCH) config.yaml

.PHONY: install-tools
install-tools:
	cd $(TOOLS_MOD_DIR) && go install golang.org/x/tools/cmd/goimports
	cd $(TOOLS_MOD_DIR) && go install github.com/golangci/golangci-lint/cmd/golangci-lint

