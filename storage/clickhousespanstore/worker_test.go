package clickhousespanstore

import (
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/gogo/protobuf/proto"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const (
	testTagCount      = 10
	testLogCount      = 5
	testLogFieldCount = 5
	testIndexTable    = "test_index_table"
	testSpansTable    = "test_spans_table"
	testTenant        = "test_tenant"
)

type expectation struct {
	preparation string
	execArgs    [][]driver.Value
}

var (
	errorMock = fmt.Errorf("error mock")
	process   = model.NewProcess("test_service", []model.KeyValue{model.String("test_process_key", "test_process_value")})
	testSpan  = model.Span{
		TraceID:       model.NewTraceID(1, 2),
		SpanID:        model.NewSpanID(3),
		OperationName: "GET /unit_test",
		StartTime:     testStartTime,
		Process:       process,
		Tags:          []model.KeyValue{model.String("test_string_key", "test_string_value"), model.Int64("test_int64_key", 4)},
		Logs:          []model.Log{{Timestamp: testStartTime, Fields: []model.KeyValue{model.String("test_log_key", "test_log_value")}}},
		Duration:      time.Minute,
	}
	testSpans             = []*model.Span{&testSpan}
	keys, values          = uniqueTagsForSpan(&testSpan)
	indexWriteExpectation = expectation{
		preparation: fmt.Sprintf("INSERT INTO %s (timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?)", testIndexTable),
		execArgs: [][]driver.Value{{
			testSpan.StartTime,
			testSpan.TraceID.String(),
			testSpan.Process.GetServiceName(),
			testSpan.OperationName,
			Microseconds(testSpan.Duration),
			keys,
			values,
		}}}
	indexWriteExpectationTenant = expectation{
		preparation: fmt.Sprintf("INSERT INTO %s (tenant, timestamp, traceID, service, operation, durationUs, tags.key, tags.value) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", testIndexTable),
		execArgs: [][]driver.Value{{
			testTenant,
			testSpan.StartTime,
			testSpan.TraceID.String(),
			testSpan.Process.GetServiceName(),
			testSpan.OperationName,
			Microseconds(testSpan.Duration),
			keys,
			values,
		}}}
	writeBatchLogs = []mocks.LogMock{{Msg: "Writing spans", Args: []interface{}{"size", len(testSpans)}}}
)

func TestSpanWriter_TagKeyValue(t *testing.T) {
	tests := map[string]struct {
		kv       model.KeyValue
		expected string
	}{
		"string value":       {kv: model.String("tag_key", "tag_string_value"), expected: "tag_string_value"},
		"true value":         {kv: model.Bool("tag_key", true), expected: "true"},
		"false value":        {kv: model.Bool("tag_key", false), expected: "false"},
		"positive int value": {kv: model.Int64("tag_key", 1203912), expected: "1203912"},
		"negative int value": {kv: model.Int64("tag_key", -1203912), expected: "-1203912"},
		"float value":        {kv: model.Float64("tag_key", 0.005009), expected: "0.005009"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, tagValue(&test.kv), "Incorrect tag value string")
		})
	}
}

func TestSpanWriter_UniqueTagsForSpan(t *testing.T) {
	tests := map[string]struct {
		tags           []model.KeyValue
		processTags    []model.KeyValue
		logs           []model.Log
		expectedKeys   []string
		expectedValues []string
	}{
		"default": {
			tags:           []model.KeyValue{model.String("key2", "value")},
			processTags:    []model.KeyValue{model.Int64("key3", 412)},
			logs:           []model.Log{{Fields: []model.KeyValue{model.Float64("key1", .5)}}},
			expectedKeys:   []string{"key1", "key2", "key3"},
			expectedValues: []string{"0.5", "value", "412"},
		},
		"repeating tags": {
			tags:           []model.KeyValue{model.String("key2", "value"), model.String("key2", "value")},
			processTags:    []model.KeyValue{model.Int64("key3", 412)},
			logs:           []model.Log{{Fields: []model.KeyValue{model.Float64("key1", .5)}}},
			expectedKeys:   []string{"key1", "key2", "key3"},
			expectedValues: []string{"0.5", "value", "412"},
		},
		"repeating keys": {
			tags:           []model.KeyValue{model.String("key2", "value_a"), model.String("key2", "value_b")},
			processTags:    []model.KeyValue{model.Int64("key3", 412)},
			logs:           []model.Log{{Fields: []model.KeyValue{model.Float64("key1", .5)}}},
			expectedKeys:   []string{"key1", "key2", "key3"},
			expectedValues: []string{"0.5", "value_a,value_b", "412"},
		},
		"repeating values": {
			tags:           []model.KeyValue{model.String("key2", "value"), model.Int64("key4", 412)},
			processTags:    []model.KeyValue{model.Int64("key3", 412)},
			logs:           []model.Log{{Fields: []model.KeyValue{model.Float64("key1", .5)}}},
			expectedKeys:   []string{"key1", "key2", "key3", "key4"},
			expectedValues: []string{"0.5", "value", "412", "412"},
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			process := model.Process{Tags: test.processTags}
			span := model.Span{Tags: test.tags, Process: &process, Logs: test.logs}
			actualKeys, actualValues := uniqueTagsForSpan(&span)
			assert.Equal(t, test.expectedKeys, actualKeys)
			assert.Equal(t, test.expectedValues, actualValues)
		})
	}
}

func TestSpanWriter_General(t *testing.T) {
	spanJSON, err := json.Marshal(&testSpan)
	require.NoError(t, err)
	modelWriteExpectationJSON := getModelWriteExpectation(spanJSON, "")
	modelWriteExpectationJSONTenant := getModelWriteExpectation(spanJSON, testTenant)
	spanProto, err := proto.Marshal(&testSpan)
	require.NoError(t, err)
	modelWriteExpectationProto := getModelWriteExpectation(spanProto, "")
	modelWriteExpectationProtoTenant := getModelWriteExpectation(spanProto, testTenant)
	tests := map[string]struct {
		encoding     Encoding
		indexTable   TableName
		tenant       string
		spans        []*model.Span
		expectations []expectation
		action       func(writeWorker *WriteWorker, spans []*model.Span) error
		expectedLogs []mocks.LogMock
	}{
		"write index batch": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        testSpans,
			expectations: []expectation{indexWriteExpectation},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeIndexBatch(spans) },
		},
		"write index tenant batch": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			tenant:       testTenant,
			spans:        testSpans,
			expectations: []expectation{indexWriteExpectationTenant},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeIndexBatch(spans) },
		},
		"write model batch JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationJSON},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeModelBatch(spans) },
		},
		"write model tenant batch JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			tenant:       testTenant,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationJSONTenant},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeModelBatch(spans) },
		},
		"write model batch Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationProto},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeModelBatch(spans) },
		},
		"write model tenant batch Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			tenant:       testTenant,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationProtoTenant},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeModelBatch(spans) },
		},
		"write batch no index JSON": {
			encoding:     EncodingJSON,
			indexTable:   "",
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationJSON},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch no index Proto": {
			encoding:     EncodingProto,
			indexTable:   "",
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationProto},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationJSON, indexWriteExpectation},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch tenant JSON": {
			encoding:     EncodingJSON,
			indexTable:   testIndexTable,
			tenant:       testTenant,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationJSONTenant, indexWriteExpectationTenant},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationProto, indexWriteExpectation},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch tenant Proto": {
			encoding:     EncodingProto,
			indexTable:   testIndexTable,
			tenant:       testTenant,
			spans:        testSpans,
			expectations: []expectation{modelWriteExpectationProtoTenant, indexWriteExpectationTenant},
			action:       func(writeWorker *WriteWorker, spans []*model.Span) error { return writeWorker.writeBatch(spans) },
			expectedLogs: writeBatchLogs,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := mocks.GetDbMock()
			require.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()

			spyLogger := mocks.NewSpyLogger()
			worker := getWriteWorker(spyLogger, db, test.encoding, test.indexTable, test.tenant)

			for _, expectation := range test.expectations {
				mock.ExpectBegin()
				prep := mock.ExpectPrepare(expectation.preparation)
				for _, args := range expectation.execArgs {
					prep.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))
				}
				mock.ExpectCommit()
			}

			assert.NoError(t, test.action(&worker, test.spans))
			assert.NoError(t, mock.ExpectationsWereMet())
			spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, test.expectedLogs)
		})
	}
}

func TestSpanWriter_BeginError(t *testing.T) {
	tests := map[string]struct {
		action       func(writeWorker *WriteWorker) error
		expectedLogs []mocks.LogMock
	}{
		"write model batch": {action: func(writeWorker *WriteWorker) error { return writeWorker.writeModelBatch(testSpans) }},
		"write index batch": {action: func(writeWorker *WriteWorker) error { return writeWorker.writeIndexBatch(testSpans) }},
		"write batch": {
			action:       func(writeWorker *WriteWorker) error { return writeWorker.writeBatch(testSpans) },
			expectedLogs: writeBatchLogs,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := mocks.GetDbMock()
			require.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()

			spyLogger := mocks.NewSpyLogger()
			writeWorker := getWriteWorker(spyLogger, db, EncodingJSON, testIndexTable, "")

			mock.ExpectBegin().WillReturnError(errorMock)

			assert.ErrorIs(t, test.action(&writeWorker), errorMock)
			assert.NoError(t, mock.ExpectationsWereMet())
			spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, test.expectedLogs)
		})
	}
}

func TestSpanWriter_PrepareError(t *testing.T) {
	spanJSON, err := json.Marshal(&testSpan)
	require.NoError(t, err)
	modelWriteExpectation := getModelWriteExpectation(spanJSON, "")
	modelWriteExpectationTenant := getModelWriteExpectation(spanJSON, testTenant)

	tests := map[string]struct {
		action       func(writeWorker *WriteWorker) error
		tenant       string
		expectation  expectation
		expectedLogs []mocks.LogMock
	}{
		"write model batch": {
			action:      func(writeWorker *WriteWorker) error { return writeWorker.writeModelBatch(testSpans) },
			expectation: modelWriteExpectation,
		},
		"write model tenant batch": {
			action:      func(writeWorker *WriteWorker) error { return writeWorker.writeModelBatch(testSpans) },
			tenant:      testTenant,
			expectation: modelWriteExpectationTenant,
		},
		"write index batch": {
			action:      func(writeWorker *WriteWorker) error { return writeWorker.writeIndexBatch(testSpans) },
			expectation: indexWriteExpectation,
		},
		"write index tenant batch": {
			action:      func(writeWorker *WriteWorker) error { return writeWorker.writeIndexBatch(testSpans) },
			tenant:      testTenant,
			expectation: indexWriteExpectationTenant,
		},
		"write batch": {
			action:       func(writeWorker *WriteWorker) error { return writeWorker.writeBatch(testSpans) },
			expectation:  modelWriteExpectation,
			expectedLogs: writeBatchLogs,
		},
		"write tenant batch": {
			action:       func(writeWorker *WriteWorker) error { return writeWorker.writeBatch(testSpans) },
			tenant:       testTenant,
			expectation:  modelWriteExpectationTenant,
			expectedLogs: writeBatchLogs,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := mocks.GetDbMock()
			require.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()

			spyLogger := mocks.NewSpyLogger()
			spanWriter := getWriteWorker(spyLogger, db, EncodingJSON, testIndexTable, test.tenant)

			mock.ExpectBegin()
			mock.ExpectPrepare(test.expectation.preparation).WillReturnError(errorMock)
			mock.ExpectRollback()

			assert.ErrorIs(t, test.action(&spanWriter), errorMock)
			assert.NoError(t, mock.ExpectationsWereMet())
			spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, test.expectedLogs)
		})
	}
}

func TestSpanWriter_ExecError(t *testing.T) {
	spanJSON, err := json.Marshal(&testSpan)
	require.NoError(t, err)
	modelWriteExpectation := getModelWriteExpectation(spanJSON, "")
	modelWriteExpectationTenant := getModelWriteExpectation(spanJSON, testTenant)
	tests := map[string]struct {
		indexTable   TableName
		tenant       string
		expectations []expectation
		action       func(writer *WriteWorker) error
		expectedLogs []mocks.LogMock
	}{
		"write model batch": {
			indexTable:   testIndexTable,
			expectations: []expectation{modelWriteExpectation},
			action:       func(writer *WriteWorker) error { return writer.writeModelBatch(testSpans) },
		},
		"write model tenant batch": {
			indexTable:   testIndexTable,
			tenant:       testTenant,
			expectations: []expectation{modelWriteExpectationTenant},
			action:       func(writer *WriteWorker) error { return writer.writeModelBatch(testSpans) },
		},
		"write index batch": {
			indexTable:   testIndexTable,
			expectations: []expectation{indexWriteExpectation},
			action:       func(writer *WriteWorker) error { return writer.writeIndexBatch(testSpans) },
		},
		"write index tenant batch": {
			indexTable:   testIndexTable,
			tenant:       testTenant,
			expectations: []expectation{indexWriteExpectationTenant},
			action:       func(writer *WriteWorker) error { return writer.writeIndexBatch(testSpans) },
		},
		"write batch no index": {
			indexTable:   "",
			expectations: []expectation{modelWriteExpectation},
			action:       func(writer *WriteWorker) error { return writer.writeBatch(testSpans) },
			expectedLogs: writeBatchLogs,
		},
		"write batch": {
			indexTable:   testIndexTable,
			expectations: []expectation{modelWriteExpectation, indexWriteExpectation},
			action:       func(writer *WriteWorker) error { return writer.writeBatch(testSpans) },
			expectedLogs: writeBatchLogs,
		},
		"write tenant batch": {
			indexTable:   testIndexTable,
			tenant:       testTenant,
			expectations: []expectation{modelWriteExpectationTenant, indexWriteExpectationTenant},
			action:       func(writer *WriteWorker) error { return writer.writeBatch(testSpans) },
			expectedLogs: writeBatchLogs,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			db, mock, err := mocks.GetDbMock()
			require.NoError(t, err, "an error was not expected when opening a stub database connection")
			defer db.Close()

			spyLogger := mocks.NewSpyLogger()
			writeWorker := getWriteWorker(spyLogger, db, EncodingJSON, testIndexTable, test.tenant)

			for i, expectation := range test.expectations {
				mock.ExpectBegin()
				prep := mock.ExpectPrepare(expectation.preparation)
				if i < len(test.expectations)-1 {
					for _, args := range expectation.execArgs {
						prep.ExpectExec().WithArgs(args...).WillReturnResult(sqlmock.NewResult(1, 1))
					}
					mock.ExpectCommit()
				} else {
					prep.ExpectExec().WithArgs(expectation.execArgs[0]...).WillReturnError(errorMock)
					mock.ExpectRollback()
				}
			}

			assert.ErrorIs(t, test.action(&writeWorker), errorMock)
			assert.NoError(t, mock.ExpectationsWereMet())
			spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, test.expectedLogs)
		})
	}
}

func getWriteWorker(spyLogger mocks.SpyLogger, db *sql.DB, encoding Encoding, indexTable TableName, tenant string) WriteWorker {
	return WriteWorker{
		params: &WorkerParams{
			logger:     spyLogger,
			db:         db,
			spansTable: testSpansTable,
			indexTable: indexTable,
			tenant:     tenant,
			encoding:   encoding,
		},
		workerDone: make(chan *WriteWorker),
	}
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

func getModelWriteExpectation(spanJSON []byte, tenant string) expectation {
	if tenant == "" {
		return expectation{
			preparation: fmt.Sprintf("INSERT INTO %s (timestamp, traceID, model) VALUES (?, ?, ?)", testSpansTable),
			execArgs: [][]driver.Value{{
				testSpan.StartTime,
				testSpan.TraceID.String(),
				spanJSON,
			}},
		}
	} else {
		return expectation{
			preparation: fmt.Sprintf("INSERT INTO %s (tenant, timestamp, traceID, model) VALUES (?, ?, ?, ?)", testSpansTable),
			execArgs: [][]driver.Value{{
				tenant,
				testSpan.StartTime,
				testSpan.TraceID.String(),
				spanJSON,
			}},
		}
	}
}
