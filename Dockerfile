FROM docker.io/library/alpine:3.16

ADD jaeger-clickhouse-linux-amd64 /go/bin/jaeger-clickhouse

RUN mkdir /plugin

# /plugin/ location is defined in jaeger-operator
CMD ["cp", "/go/bin/jaeger-clickhouse", "/plugin/jaeger-clickhouse"]
