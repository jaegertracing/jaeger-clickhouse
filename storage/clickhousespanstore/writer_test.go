package clickhousespanstore

import (
	"database/sql"
	"database/sql/driver"
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

	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const (
	testSpanCount     = 100
	testTagCount      = 10
	testLogCount      = 5
	testLogFieldCount = 5
	testIndexTable    = "test_index_table"
	testSpansTable    = "test_spans_table"
)

type expectation struct {
	preparation string
	execArgs    [][]driver.Value
}

var (
	errorMock            = fmt.Errorf("error mock")
	encodingsAndMarshals = map[string]struct {
		encoding Encoding
		marshal  func(span *model.Span) ([]byte, error)
	}{
		"json":     {encoding: EncodingJSON, marshal: func(span *model.Span) ([]byte, error) { return json.Marshal(span) }},
		"protobuf": {encoding: EncodingProto, marshal: func(span *model.Span) ([]byte, error) { return proto.Marshal(span) }},
	}
	process = model.NewProcess("test_service", []model.KeyValue{model.String("test_process_key", "test_process_value")})
	span    = model.Span{
		TraceID:       model.NewTraceID(1, 2),
		SpanID:        model.NewSpanID(3),
		OperationName: "GET /unit_test",
		StartTime:     testStartTime,
		Process:       process,
		Tags:          []model.KeyValue{model.String("test_string_key", "test_string_value"), model.Int64("test_int64_key", 4)},
		Logs:          []model.Log{{Timestamp: testStartTime, Fields: []model.KeyValue{model.String("test_log_key", "test_log_value")}}},
		Duration:      time.Minute,
	}
	spans = []*model.Span{&span}
	tags = uniqueTagsForSpan(&span)
	indexWriteExpectation = expectation{
		preparation: fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags) VALUES (?, ?, ?, ?, ?, ?)", testIndexTable),
		execArgs: [][]driver.Value{{
			span.StartTime,
			span.TraceID.String(),
			span.Process.GetServiceName(),
			span.OperationName,
			span.Duration.Microseconds(),
			tags,
		}}}
)

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
	spans := generateRandomSpans(testSpanCount)
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

func TestSpanWriter_General(t *testing.T) {
	spanJSON, err := json.Marshal(&span)
	require.NoError(t, err)
	modelWriteExpectationJSON := getModelWriteExpectation(spanJSON)
	spanProto, err := proto.Marshal(&span)
	require.NoError(t, err)
	modelWriteExpectationProto := getModelWriteExpectation(spanProto)
	tests := map[string]struct {
		encoding     Encoding
		indexTable   TableName
		spans        []*model.Span
		expectations []expectation
		action       func(writer *SpanWriter, spans []*model.Span) error
		expectedLogs []mocks.LogMock
	}{
		"write index batch": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        spans,
			expectations: []expectation{indexWriteExpectation},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeIndexBatch(spans)
			},
		},
		"write model batch JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        spans,
			expectations: []expectation{modelWriteExpectationJSON},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeModelBatch(spans)
			},
		},
		"write model bach Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			spans:        spans,
			expectations: []expectation{modelWriteExpectationProto},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeModelBatch(spans)
			},
		},
		"write batch no index JSON": {
			encoding:     EncodingJSON,
			indexTable:   "",
			spans:        spans,
			expectations: []expectation{modelWriteExpectationJSON},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeBatch(spans)
			},
			expectedLogs: []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", len(spans)}}},
		},
		"write batch no index Proto": {
			encoding:     EncodingProto,
			indexTable:   "",
			spans:        spans,
			expectations: []expectation{modelWriteExpectationProto},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeBatch(spans)
			},
			expectedLogs: []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", len(spans)}}},
		},
		"write batch JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        spans,
			expectations: []expectation{modelWriteExpectationJSON, indexWriteExpectation},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeBatch(spans)
			},
			expectedLogs: []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", len(spans)}}},
		},
		"write batch Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			spans:        spans,
			expectations: []expectation{modelWriteExpectationProto, indexWriteExpectation},
			action: func(writer *SpanWriter, spans []*model.Span) error {
				return writer.writeBatch(spans)
			},
			expectedLogs: []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", len(spans)}}},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := getDbMock()
			require.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()

			spyLogger := mocks.NewSpyLogger()
			spanWriter := getSpanWriter(spyLogger, db, test.encoding, test.indexTable)

			for _, expectation := range test.expectations {
				mock.ExpectBegin()
				prep := mock.ExpectPrepare(expectation.preparation)
				for _, args := range expectation.execArgs {
					prep.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectCommit()
			}

			assert.NoError(t, test.action(spanWriter, test.spans), "Could not write spans")
			assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
			spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, test.expectedLogs)
		})
	}
}

func TestSpanWriter_WriteBatchModelError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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

func TestSpanWriter_WriteBatchExecError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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

func TestSpanWriter_WriteModelBatchBeginError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

	mock.ExpectBegin()

	span := generateRandomSpan()
	_ = mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable)).WillReturnError(errorMock)

	mock.ExpectRollback()

	assert.EqualError(t, spanWriter.writeModelBatch([]*model.Span{&span}), errorMock.Error())
	assert.NoError(t, mock.ExpectationsWereMet(), "Not all expected queries were made")
	spyLogger.AssertLogsEmpty(t)
}

func TestSpanWriterWriteModelBatchExecuteError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	span := generateRandomSpan()
	for name, test := range encodingsAndMarshals {
		t.Run(name, func(t *testing.T) {
			spyLogger := mocks.NewSpyLogger()
			spanWriter := getSpanWriter(spyLogger, db, test.encoding, testIndexTable)

			mock.ExpectBegin()
			prep := mock.ExpectPrepare(fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", spanWriter.spansTable))

			serializedSpan, err := test.marshal(&span)
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
		})
	}
}

func TestSpanWriter_WriteIndexBatchBeginError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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
	spanWriter := getSpanWriter(spyLogger, db, EncodingJSON, testIndexTable)

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

func getDbMock() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(mocks.ConverterMock{}),
	)
}

func getSpanWriter(spyLogger mocks.SpyLogger, db *sql.DB, encoding Encoding, indexTable TableName) *SpanWriter {
	return NewSpanWriter(
		spyLogger,
		db,
		indexTable,
		testSpansTable,
		encoding,
		0,
		0,
	)
}

func generateRandomSpans(count int) []*model.Span {
	spans := make([]*model.Span, count)
	for i := 0; i < count; i++ {
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

func getModelWriteExpectation(spanJSON []byte) expectation {
	return expectation{
		preparation: fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", testSpansTable),
		execArgs: [][]driver.Value{{
			span.StartTime,
			span.TraceID.String(),
			spanJSON,
		}},
	}
}
