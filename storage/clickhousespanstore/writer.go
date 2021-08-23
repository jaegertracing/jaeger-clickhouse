package clickhousespanstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gogo/protobuf/proto"
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
	sizeAddition  int64    = 1000
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
	logger     hclog.Logger
	db         *sql.DB
	indexTable TableName
	spansTable TableName
	encoding   Encoding
	delay      time.Duration
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
		logger:     logger,
		db:         db,
		indexTable: indexTable,
		spansTable: spansTable,
		encoding:   encoding,
		delay:      delay,
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
	batch := make([]*model.Span, 0, w.size)
	maxSize := 4 * w.size

	timer := time.After(w.delay)
	last := time.Now()

	for {
		w.done.Add(1)

		flush := false
		finish := false

		select {
		case span := <-w.spans:
			batch = append(batch, span)
			flush = len(batch) == cap(batch)
			if flush {
				w.logger.Debug("Flush due to batch size", "size", len(batch))
				numWritesWithBatchSize.Inc()
			}
		case <-timer:
			timer = time.After(w.delay)
			flush = time.Since(last) > w.delay && len(batch) > 0
			if flush {
				w.logger.Debug("Flush due to timer")
				numWritesWithFlushInterval.Inc()
			}
		case <-w.finish:
			finish = true
			flush = len(batch) > 0
			w.logger.Debug("Finish channel")
		}

		if flush {
			err := w.writeBatch(batch)
			if err != nil {
				w.logger.Error("Could not write a batch of spans", "error", err)
			}
			if len(batch) == cap(batch) {
				if err != nil {
					w.size /= 2
				} else {
					w.size += sizeAddition
					if w.size > maxSize {
						w.size = maxSize
					}
				}
			}

			batch = make([]*model.Span, 0, w.size)
			last = time.Now()
		}

		w.done.Done()

		if finish {
			break
		}
	}
}

func (w *SpanWriter) writeBatch(batch []*model.Span) error {
	w.logger.Debug("Writing spans", "size", len(batch))
	if err := w.writeModelBatch(batch); err != nil {
		return err
	}

	if w.indexTable != "" {
		if err := w.writeIndexBatch(batch); err != nil {
			return err
		}
	}

	return nil
}

func (w *SpanWriter) writeModelBatch(batch []*model.Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", w.spansTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		var serialized []byte

		if w.encoding == EncodingJSON {
			serialized, err = json.Marshal(span)
		} else {
			serialized, err = proto.Marshal(span)
		}

		if err != nil {
			return err
		}

		_, err = statement.Exec(span.StartTime, span.TraceID.String(), serialized)
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
}

func (w *SpanWriter) writeIndexBatch(batch []*model.Span) error {
	tx, err := w.db.Begin()
	if err != nil {
		return err
	}

	committed := false

	defer func() {
		if !committed {
			// Clickhouse does not support real rollback
			_ = tx.Rollback()
		}
	}()

	statement, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?)", w.indexTable))
	if err != nil {
		return err
	}

	defer statement.Close()

	for _, span := range batch {
		keys, values := uniqueTagsForSpan(span)
		_, err = statement.Exec(
			span.StartTime,
			span.TraceID.String(),
			span.Process.ServiceName,
			span.OperationName,
			span.Duration.Microseconds(),
			keys,
			values,
		)
		if err != nil {
			return err
		}
	}

	committed = true

	return tx.Commit()
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

type kvArray []*model.KeyValue

func (arr kvArray) Len() int {
	return len(arr)
}

func (arr kvArray) Swap(i, j int) {
	if i < 0 || i >= arr.Len() || j < 0 || j > arr.Len() {
		panic(fmt.Errorf("indices are incorrect"))
	}
	arr[i], arr[j] = arr[j], arr[i]
}

func (arr kvArray) Less(i, j int) bool {
	if i < 0 || i >= arr.Len() || j < 0 || j > arr.Len() {
		panic(fmt.Errorf("indices are incorrect"))
	}
	return arr[i].Key < arr[j].Key || (arr[i].Key == arr[j].Key && arr[i].AsString() < arr[j].AsString())
}

func uniqueTagsForSpan(span *model.Span) (keys, values []string) {
	uniqueTags := make(map[string]*model.KeyValue, len(span.Tags)+len(span.Process.Tags))

	for i := range span.Tags {
		uniqueTags[tagString(&span.GetTags()[i])] = &span.GetTags()[i]
	}

	for i := range span.Process.Tags {
		uniqueTags[tagString(&span.GetProcess().GetTags()[i])] = &span.GetProcess().GetTags()[i]
	}

	for _, event := range span.Logs {
		for i := range event.Fields {
			uniqueTags[tagString(&event.GetFields()[i])] = &event.GetFields()[i]
		}
	}

	uniqueTagsSlice := make(kvArray, 0, len(uniqueTags))
	for _, kv := range uniqueTags {
		uniqueTagsSlice = append(uniqueTagsSlice, kv)
	}
	sort.Sort(uniqueTagsSlice)

	keys = make([]string, 0, len(uniqueTags))
	values = make([]string, 0, len(uniqueTags))
	for _, tws := range uniqueTagsSlice {
		keys = append(keys, tws.Key)
		values = append(values, tws.AsString())
	}

	return keys, values
}

func tagString(kv *model.KeyValue) string {
	return kv.Key + "=" + kv.AsString()
}
