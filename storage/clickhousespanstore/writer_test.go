package clickhousespanstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/hashicorp/go-hclog"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const (
	testSpanCount  = 100
	testTagCount   = 100
	testIndexTable = "test_index_table"
	testSpansTable = "test_spans_table"
)

func TestSpanWriter_TagString(t *testing.T) {
	tags := generateRandomTags()
	for i := range tags {
		kv := tags[i]
		want := fmt.Sprintf("%s=%s", kv.Key, kv.AsString())
		got := tagString(&kv)
		if got != want {
			t.Fatalf("Incorrect tag string, want %s, got %s", want, got)
		}
	}
}

func TestSpanWriter_UniqueTagsForSpan(t *testing.T) {
	spans := generateRandomSpans()
	for _, span := range spans {
		uniqueTags := make(map[string]struct{}, len(span.Tags)+len(span.Process.Tags))
		for i := range span.Tags {
			uniqueTags[tagString(&span.Tags[i])] = struct{}{}
		}
		for i := range span.Process.Tags {
			uniqueTags[tagString(&span.Process.Tags[i])] = struct{}{}
		}
		want := make([]string, 0, len(uniqueTags))
		for tag := range uniqueTags {
			want = append(want, tag)
		}
		sort.Strings(want)

		got := uniqueTagsForSpan(span)

		assert.Equal(t, want, got)
	}
}

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
	spanWriter := NewSpanWriter(
		spyLogger,
		db,
		"",
		testSpansTable,
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
	require.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")

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
	spanWriter := getSpanWriter(spyLogger, db, encoding)
	spans := generateRandomSpans()
	if err = expectModelWritten(mock, spans, marshal, spanWriter); err != nil {
		t.Fatalf("could not expect queries due to %s", err)
	}
	expectIndexWritten(mock, spans, spanWriter)
	if err = spanWriter.writeBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
	require.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")

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
	spanWriter := getSpanWriter(spyLogger, db, encoding)

	spans := generateRandomSpans()
	if err = expectModelWritten(mock, spans, marshal, spanWriter); err != nil {
		t.Fatalf("could not expect queries due to %s", err)
	}
	if err = spanWriter.writeModelBatch(spans); err != nil {
		t.Fatalf("could not write spans due to error: %s", err)
	}
	require.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteIndexBatch(t *testing.T) {
	db, mock, err := getDbMock()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	spans := generateRandomSpans()
	expectIndexWritten(mock, spans, spanWriter)
	if err = spanWriter.writeIndexBatch(spans); err != nil {
		t.Fatalf("Could not write spans due to error: %s", err)
	}
	require.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
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

func getSpanWriter(spyLogger mocks.SpyLogger, db *sql.DB, encoding Encoding) *SpanWriter {
	return NewSpanWriter(
		spyLogger,
		db,
		testIndexTable,
		testSpansTable,
		encoding,
		0,
		0,
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
	processTags := generateRandomTags()
	process := model.Process{
		ServiceName: "service" + strconv.FormatUint(rand.Uint64(), 10),
		Tags:        processTags,
	}
	span := model.Span{
		TraceID:       model.NewTraceID(rand.Uint64(), rand.Uint64()),
		SpanID:        model.NewSpanID(rand.Uint64()),
		OperationName: "operation" + strconv.FormatUint(rand.Uint64(), 10),
		StartTime:     time.Unix(rand.Int63n(time.Now().Unix()), 0),
		Process:       &process,
		Tags:          generateRandomTags(),
		Duration:      time.Unix(rand.Int63n(1<<32), 0).Sub(time.Unix(0, 0)),
	}
	return span
}

func generateRandomTags() []model.KeyValue {
	tags := make([]model.KeyValue, 0, testTagCount)
	for i := 0; i < testTagCount; i++ {
		key := "key" + strconv.FormatUint(rand.Uint64(), 16)
		value := "key" + strconv.FormatUint(rand.Uint64(), 16)
		kv := model.KeyValue{Key: key, VType: model.ValueType_STRING, VStr: value}
		tags = append(tags, kv)
	}

	return tags
}
