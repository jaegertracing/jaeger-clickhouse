package clickhousespanstore

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/prometheus/client_golang/prometheus"
)

type Encoding string

const (
	// EncodingJSON is used for spans encoded as JSON.
	EncodingJSON Encoding = "json"
	// EncodingProto is used for spans encoded as Protobuf.
	EncodingProto Encoding = "protobuf"
)

var (
	numWritesWithBatchSize = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "jaeger_clickhouse_writes_with_batch_size_total",
		Help: "Number of clickhouse writes due to batch size criteria",
	})
	numWritesWithFlushInterval = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "jaeger_clickhouse_writes_with_flush_interval_total",
		Help: "Number of clickhouse writes due to flush interval criteria",
	})
)

// SpanWriter for writing spans to ClickHouse
type SpanWriter struct {
	writeParams WriteParams

	size       int64
	spans      chan *model.Span
	finish     chan bool
	done       sync.WaitGroup
}

var registerMetrics sync.Once
var _ spanstore.Writer = (*SpanWriter)(nil)

// NewSpanWriter returns a SpanWriter for the database
func NewSpanWriter(logger hclog.Logger, db *sql.DB, indexTable, spansTable TableName, encoding Encoding, delay time.Duration, size int64) *SpanWriter {
	writer := &SpanWriter{
		writeParams: WriteParams{
			logger:     logger,
			db:         db,
			indexTable: indexTable,
			spansTable: spansTable,
			encoding:   encoding,
			delay:      delay,
		},
		size:       size,
		spans:      make(chan *model.Span, size),
		finish:     make(chan bool),
	}

	writer.registerMetrics()
	go writer.backgroundWriter()

	return writer
}

func (w *SpanWriter) registerMetrics() {
	registerMetrics.Do(func() {
		prometheus.MustRegister(numWritesWithBatchSize)
		prometheus.MustRegister(numWritesWithFlushInterval)
	})
}

func (w *SpanWriter) backgroundWriter() {
	pool := NewWorkerPool(&w.writeParams)
	go pool.Work()
	batch := make([]*model.Span, 0, w.size)

	timer := time.After(w.writeParams.delay)
	last := time.Now()

	for {
		// TODO: do something with w.done
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
			if flush {
				w.writeParams.logger.Debug("Flush due to batch size", "size", len(batch))
				numWritesWithBatchSize.Inc()
			}
		case <-timer:
			timer = time.After(w.writeParams.delay)
			flush = time.Since(last) > w.writeParams.delay && len(batch) > 0
			if flush {
				w.writeParams.logger.Debug("Flush due to timer")
				numWritesWithFlushInterval.Inc()
			}
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
			pool.finish <- true
			pool.done.Wait()
			w.writeParams.logger.Debug("Finish channel")
		}

		if flush {
			pool.WriteBatch(batch)

			batch = make([]*model.Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

// WriteSpan writes the encoded span
func (w *SpanWriter) WriteSpan(_ context.Context, span *model.Span) error {
	w.spans <- span
	return nil
}

// Close Implements io.Closer and closes the underlying storage
func (w *SpanWriter) Close() error {
	w.finish <- true
	w.done.Wait()
	return nil
}
