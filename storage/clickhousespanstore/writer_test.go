package clickhousespanstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"

	"github.com/hashicorp/go-hclog"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	"github.com/jaegertracing/jaeger/model"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const (
	testSpanCount     = 100
	testTagCount      = 10
	testLogCount      = 5
	testLogFieldCount = 5
	testIndexTable    = "test_index_table"
	testSpansTable    = "test_spans_table"
)

var errorMock = fmt.Errorf("error mock")

func TestSpanWriter_TagString(t *testing.T) {
	tests := map[string]struct {
		kv       model.KeyValue
		expected string
	}{
		"string value":       {kv: model.String("tag_key", "tag_string_value"), expected: "tag_key=tag_string_value"},
		"true value":         {kv: model.Bool("tag_key", true), expected: "tag_key=true"},
		"false value":        {kv: model.Bool("tag_key", false), expected: "tag_key=false"},
		"positive int value": {kv: model.Int64("tag_key", 1203912), expected: "tag_key=1203912"},
		"negative int value": {kv: model.Int64("tag_key", -1203912), expected: "tag_key=-1203912"},
		"float value":        {kv: model.Float64("tag_key", 0.005009), expected: "tag_key=0.005009"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, tagString(&test.kv), "Incorrect tag string")
		})
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
		for _, log := range span.Logs {
			for i := range log.Fields {
				uniqueTags[tagString(&log.Fields[i])] = struct{}{}
			}
		}
		want := make([]string, 0, len(uniqueTags))
		for tag := range uniqueTags {
			want = append(want, tag)
		}
		sort.Strings(want)

		assert.Equal(t, want, uniqueTagsForSpan(span))
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
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
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
	require.NoError(t, expectModelWritten(mock, spans, marshal, spanWriter), "could not expect queries due to %s", err)
	assert.NoError(t, spanWriter.writeBatch(spans), "Could not write spans")
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")

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
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, encoding)
	spans := generateRandomSpans()
	require.NoError(t, expectModelWritten(mock, spans, marshal, spanWriter), "could not expect queries due to %s", err)
	expectIndexWritten(mock, spans, spanWriter)
	assert.NoError(t, spanWriter.writeBatch(spans), "Could not write spans")
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")

	spyLogger.AssertLogsOfLevelEqual(
		t,
		hclog.Debug,
		[]mocks.LogMock{
			{Msg: "Writing spans", Args: []interface{}{"size", testSpanCount}},
		},
	)
}

func TestSpanWriter_WriteBatchModelError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable))

	span := generateRandomSpan()
	serializedSpan, err := json.Marshal(span)
	require.NoError(t, err, "could not marshal span", span)

	prep.
		ExpectExec().
		WithArgs(
			span.StartTime,
			span.TraceID.String(),
			serializedSpan,
		).
		WillReturnError(errorMock)
	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", 1}}})
}

func TestSpanWriter_WriteBatchIndexError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable))

	span := generateRandomSpan()
	serializedSpan, err := json.Marshal(span)
	require.NoError(t, err, "could not marshal span", span)

	prep.
		ExpectExec().
		WithArgs(
			span.StartTime,
			span.TraceID.String(),
			serializedSpan,
		).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	mock.ExpectBegin()
	prep = mock.ExpectPrepare(fmt.Sprintf(
		"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)",
		spanWriter.indexTable,
	))
	prep.ExpectExec().WithArgs(
		span.StartTime,
		span.TraceID,
		span.Process.ServiceName,
		span.OperationName,
		span.Duration.Microseconds(),
		fmt.Sprint(uniqueTagsForSpan(&span)),
	).WillReturnError(errorMock)
	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", 1}}})
}

func TestSpanWriter_WriteModelBatchJSON(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteModelBatchProtobuf(t *testing.T) {
	testSpanWriterWriteModelBatch(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteModelBatch(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, encoding)

	spans := generateRandomSpans()
	require.NoError(t, expectModelWritten(mock, spans, marshal, spanWriter), "could not expect queries due to %s", err)
	assert.NoError(t, spanWriter.writeModelBatch(spans), "Could not write spans")
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteModelBatchBeginError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin().WillReturnError(errorMock)

	span := generateRandomSpan()

	assert.EqualError(t, spanWriter.writeIndexBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
}

func TestSpanWriter_WriteModelBatchPrepareError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin()

	span := generateRandomSpan()
	_ = mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable)).WillReturnError(errorMock)

	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeModelBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteModelBatchJSONExecuteError(t *testing.T) {
	testSpanWriterWriteModelBatchExecuteError(t, EncodingJSON, func(span *model.Span) ([]byte, error) { return json.Marshal(span) })
}

func TestSpanWriter_WriteModelBatchProtobufExecuteError(t *testing.T) {
	testSpanWriterWriteModelBatchExecuteError(t, EncodingProto, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) })
}

func testSpanWriterWriteModelBatchExecuteError(t *testing.T, encoding Encoding, marshal func(span *model.Span) ([]byte, error)) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, encoding)

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable))

	span := generateRandomSpan()
	serializedSpan, err := marshal(&span)
	require.NoError(t, err, "could not marshal span", span)

	prep.
		ExpectExec().
		WithArgs(
			span.StartTime,
			span.TraceID.String(),
			serializedSpan,
		).
		WillReturnError(errorMock)
	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeModelBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteIndexBatch(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	spans := generateRandomSpans()
	expectIndexWritten(mock, spans, spanWriter)
	assert.NoError(t, spanWriter.writeIndexBatch(spans), "Could not write spans")
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteIndexBatchBeginError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin().WillReturnError(errorMock)

	span := generateRandomSpan()

	assert.EqualError(t, spanWriter.writeIndexBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
}

func TestSpanWriter_WriteIndexBatchPrepareError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin()

	span := generateRandomSpan()
	_ = mock.ExpectPrepare(fmt.Sprintf(
		"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)",
		spanWriter.indexTable,
	)).WillReturnError(errorMock)

	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeIndexBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriter_WriteIndexBatchExecuteError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON)

	mock.ExpectBegin()
	prep := mock.ExpectPrepare(fmt.Sprintf(
		"INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)",
		spanWriter.indexTable,
	))

	span := generateRandomSpan()
	prep.ExpectExec().WithArgs(
		span.StartTime,
		span.TraceID,
		span.Process.ServiceName,
		span.OperationName,
		span.Duration.Microseconds(),
		fmt.Sprint(uniqueTagsForSpan(&span)),
	).WillReturnError(errorMock)
	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeIndexBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
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
	processTags := generateRandomKeyValues(testTagCount)
	process := model.Process{
		ServiceName: "service" + strconv.FormatUint(rand.Uint64(), 10),
		Tags:        processTags,
	}
	span := model.Span{
		TraceID:       model.NewTraceID(rand.Uint64(), rand.Uint64()),
		SpanID:        model.NewSpanID(rand.Uint64()),
		OperationName: "operation" + strconv.FormatUint(rand.Uint64(), 10),
		StartTime:     getRandomTime(),
		Process:       &process,
		Tags:          generateRandomKeyValues(testTagCount),
		Logs:          generateRandomLogs(),
		Duration:      time.Unix(rand.Int63n(1<<32), 0).Sub(time.Unix(0, 0)),
	}
	return span
}

func generateRandomLogs() []model.Log {
	logs := make([]model.Log, 0, testLogCount)
	for i := 0; i < testLogCount; i++ {
		timestamp := getRandomTime()
		logs = append(logs, model.Log{Timestamp: timestamp, Fields: generateRandomKeyValues(testLogFieldCount)})
	}
	return logs
}

func getRandomTime() time.Time {
	return time.Unix(rand.Int63n(time.Now().Unix()), 0)
}

func generateRandomKeyValues(count int) []model.KeyValue {
	tags := make([]model.KeyValue, 0, count)
	for i := 0; i < count; i++ {
		key := "key" + strconv.FormatUint(rand.Uint64(), 16)
		value := "key" + strconv.FormatUint(rand.Uint64(), 16)
		kv := model.KeyValue{Key: key, VType: model.ValueType_STRING, VStr: value}
		tags = append(tags, kv)
	}

	return tags
}
