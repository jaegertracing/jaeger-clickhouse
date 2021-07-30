package clickhousespanstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const testSpanCount = 100

func TestSpanWriter_WriteBatchNoIndexJSON(t *testing.T) {
	testSpanWriterWriteBatchNoIndex(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteBatchNoIndexProto(t *testing.T) {
	testSpanWriterWriteBatchNoIndex(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteBatchNoIndex(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := getDbMock()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	indexTable := ""
	spansTable := "test_spans_table"
	spanWriter := NewSpanWriter(
		spyLogger,
		db,
		indexTable,
		spansTable,
		encoding,
		0,
		0,
	)

	spans := generateRandomSpans()
	if err = expectModelWritten(mock, spans, marshal, spanWriter); err != nil {
		t.Fatalf("could not expect queries due to %s", err)
	}
	if err = spanWriter.writeBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
	assertExpectationsWereMet(t, mock)

	spyLogger.AssertLogsOfLevelEqual(
		t,
		hclog.Debug,
		[]mocks.LogMock{
			{Msg: "Writing spans", Args: []interface{}{"size", testSpanCount}},
		},
	)
}

func TestSpanWriter_WriteBatchJSON(t *testing.T) {
	testSpanWriterWriteBatch(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteBatchProto(t *testing.T) {
	testSpanWriterWriteBatch(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteBatch(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := getDbMock()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	indexTable := "test_index_table"
	spansTable := "test_spans_table"
	spanWriter := NewSpanWriter(
		spyLogger,
		db,
		indexTable,
		spansTable,
		encoding,
		0,
		0,
	)

	spans := generateRandomSpans()
	if err = expectModelWritten(mock, spans, marshal, spanWriter); err != nil {
		t.Fatalf("could not expect queries due to %s", err)
	}
	expectIndexWritten(mock, spans, spanWriter)
	if err = spanWriter.writeBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
	assertExpectationsWereMet(t, mock)

	spyLogger.AssertLogsOfLevelEqual(
		t,
		hclog.Debug,
		[]mocks.LogMock{
			{Msg: "Writing spans", Args: []interface{}{"size", testSpanCount}},
		},
	)
}

func TestSpanWriter_WriteModelBatchJSON(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteModelBatchProtobuf(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteModelBatch(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := getDbMock()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
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

	spans := generateRandomSpans()
	if err = expectModelWritten(mock, spans, marshal, spanWriter); err != nil {
		t.Fatalf("could not expect queries due to %s", err)
	}
	if err = spanWriter.writeModelBatch(spans); err != nil {
		t.Fatalf("could not write spans due to error: %s", err)
	}
	assertExpectationsWereMet(t, mock)
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteIndexBatch(t *testing.T) {
	db, mock, err := getDbMock()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	indexTable := "test_index_table"
	spanWriter := NewSpanWriter(
		spyLogger,
		db,
		indexTable,
		"",
		EncodingJSON,
		0,
		0,
	)

	spans := generateRandomSpans()
	expectIndexWritten(mock, spans, spanWriter)
	if err = spanWriter.writeIndexBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
	assertExpectationsWereMet(t, mock)
	spyLogger.AssertLogsEmpty(t)
}

func expectModelWritten(
	mock sqlmock.Sqlmock,
	spans []*model.Span,
	marshal func(span *model.Span) ([]byte, error),
	spanWriter *SpanWriter,
) error {
	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable))
	for _, span := range spans {
		serializedSpan, err := marshal(span)
		if err != nil {
			return fmt.Errorf("could not marshal %s due to %s", fmt.Sprint(span), err)
		}

		prep.
			ExpectExec().
			WithArgs(
				span.StartTime,
				span.TraceID.String(),
				serializedSpan,
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
	return nil
}

func expectIndexWritten(
	mock sqlmock.Sqlmock,
	spans []*model.Span,
	spanWriter *SpanWriter,
) {
	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf(
		"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)",
		spanWriter.indexTable,
	))
	for _, span := range spans {
		prep.
			ExpectExec().
			WithArgs(
				span.StartTime,
				span.TraceID,
				span.Process.ServiceName,
				span.OperationName,
				span.Duration.Microseconds(),
				fmt.Sprint(uniqueTagsForSpan(span)),
			).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
}

func getDbMock() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(mocks.ConverterMock{}),
	)
}

func generateRandomSpans() []*model.Span {
	spans := make([]*model.Span, testSpanCount)
	for i := 0; i < testSpanCount; i++ {
		span := generateRandomSpan()
		spans[i] = &span
	}
	return spans
}

func generateRandomSpan() model.Span {
	process := model.Process{ServiceName: "service" + strconv.FormatUint(rand.Uint64(), 10)}
	span := model.Span{
		TraceID:       model.NewTraceID(rand.Uint64(), rand.Uint64()),
		SpanID:        model.NewSpanID(rand.Uint64()),
		OperationName: "operation" + strconv.FormatUint(rand.Uint64(), 10),
		StartTime:     time.Unix(rand.Int63n(time.Now().Unix()), 0),
		Process:       &process,
		Duration:      time.Unix(rand.Int63n(1<<32), 0).Sub(time.Unix(0, 0)),
	}
	return span
}

func assertExpectationsWereMet(t *testing.T, mock sqlmock.Sqlmock) {
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("Not all expected queries were made")
	}
}
