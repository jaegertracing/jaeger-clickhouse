package clickhousespanstore

import (
	"encoding/json"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
	"math/rand"
	"strconv"
	"testing"
	"time"
)

func TestSpanWriter_WriteModelBatchJSON(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteModelBatchProtobuf(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteModelBatch(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.SpyLogger{}
	spansTable := "test_spans_table"
	spanWriter := NewSpanWriter(
		spyLogger,
		db,
		"",
		spansTable,
		encoding,
		0,
		0,
	)

	mock.ExpectBegin()

	spans := make([]*model.Span, 1000)
	for i := 0; i < 1000; i++ {
		span := model.Span{
			TraceID:       model.NewTraceID(rand.Uint64(), rand.Uint64()),
			SpanID:        model.NewSpanID(rand.Uint64()),
			OperationName: "operation" + strconv.FormatUint(rand.Uint64(), 10),
			StartTime:     time.Unix(rand.Int63n(time.Now().Unix()), 0),
		}
		spans[i] = &span
		serializedSpan, err := marshal(&span)
		if err != nil {
			t.Fatalf("Could not marshal %s due to %s", fmt.Sprint(span), err)
		}

		mock.ExpectExec(fmt.Sprintf(
			"INSERT INTO %s (timestamp, trace_id, model) VALUES (%s, %s, %s)",
			spansTable,
			span.StartTime,
			span.TraceID,
			serializedSpan,
		))
	}

	mock.ExpectCommit()

	if err = spanWriter.writeModelBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
}
