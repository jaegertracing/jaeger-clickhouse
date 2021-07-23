FROM alpine:3.13

ADD jaeger-clickhouse-linux-amd64 /go/bin/jaeger-clickhouse

RUN mkdir /plugin

# /plugin/ location is defined in jaeger-operator
CMD ["cp", "/go/bin/jaeger-clickhouse", "/plugin/jaeger-clickhouse"]
