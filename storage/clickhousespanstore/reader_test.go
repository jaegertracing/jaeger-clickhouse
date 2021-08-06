package clickhousespanstore

import (
	"context"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testOperationsTable = "test_operations_table"
	testNumTraces       = 10
)

func TestSpanWriter_getTraces(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)
	traceIDs := []model.TraceID{
		{High: 0, Low: 1},
		{High: 2, Low: 2},
		{High: 1, Low: 3},
		{High: 0, Low: 4},
	}
	spans := make([]model.Span, 2*len(traceIDs))
	for i := 0; i < 2*len(traceIDs); i++ {
		traceID := traceIDs[i%len(traceIDs)]
		spans[i] = generateRandomSpan()
		spans[i].TraceID = traceID
	}

	traceIDStrings := make([]driver.Value, 4)
	for i, traceID := range traceIDs {
		traceIDStrings[i] = traceID.String()
	}

	tests := map[string]struct {
		queryResult    *sqlmock.Rows
		expectedTraces []*model.Trace
	}{
		"JSON encoded traces one span per trace": {
			queryResult:    getEncodedSpans(spans[:len(traceIDs)], func(span *model.Span) ([]byte, error) { return json.Marshal(span) }),
			expectedTraces: getTracesFromSpans(spans[:len(traceIDs)]),
		},
		"Protobuf encoded traces one span per trace": {
			queryResult:    getEncodedSpans(spans[:len(traceIDs)], func(span *model.Span) ([]byte, error) { return proto.Marshal(span) }),
			expectedTraces: getTracesFromSpans(spans[:len(traceIDs)]),
		},
		"JSON encoded traces many spans per trace": {
			queryResult:    getEncodedSpans(spans, func(span *model.Span) ([]byte, error) { return json.Marshal(span) }),
			expectedTraces: getTracesFromSpans(spans),
		},
		"Protobuf encoded traces many spans per trace": {
			queryResult:    getEncodedSpans(spans, func(span *model.Span) ([]byte, error) { return proto.Marshal(span) }),
			expectedTraces: getTracesFromSpans(spans),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			mock.
				ExpectQuery(
					fmt.Sprintf("SELECT model FROM %s PREWHERE traceID IN (?,?,?,?)", testSpansTable),
				).
				WithArgs(traceIDStrings...).
				WillReturnRows(test.queryResult)

			traces, err := traceReader.getTraces(context.Background(), traceIDs)
			require.NoError(t, err)
			model.SortTraces(traces)
			assert.Equal(t, test.expectedTraces, traces)
		})
	}
}

func getEncodedSpans(spans []model.Span, marshal func(span *model.Span) ([]byte, error)) *sqlmock.Rows {
	rows := sqlmock.NewRows([]string{"model"})
	for i := range spans {
		serialized, err := marshal(&spans[i])
		if err != nil {
			panic(err)
		}
		rows.AddRow(serialized)
	}
	return rows
}

func getTracesFromSpans(spans []model.Span) []*model.Trace {
	traces := make(map[model.TraceID]*model.Trace)
	for i, span := range spans {
		if _, ok := traces[span.TraceID]; !ok {
			traces[span.TraceID] = &model.Trace{}
		}
		traces[span.TraceID].Spans = append(traces[span.TraceID].Spans, &spans[i])
	}

	res := make([]*model.Trace, 0, len(traces))
	for _, trace := range traces {
		res = append(res, trace)
	}
	model.SortTraces(res)
	return res
}

func TestSpanWriter_findTraceIDsInRange(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)
	service := "test_service"
	operation := "test_operation"
	start := time.Unix(0, 0)
	end := time.Now()
	minDuration := time.Minute
	maxDuration := time.Hour
	tags := map[string]string{
		"key": "value",
	}
	skip := []model.TraceID{
		{High: 1, Low: 1},
		{High: 0, Low: 0},
	}
	tagArgs := func(tags map[string]string) []string {
		res := make([]string, 0, len(tags))
		for key, value := range tags {
			res = append(res, fmt.Sprintf("%s=%s", key, value))
		}
		return res
	}(tags)
	rowValues := []driver.Value{
		"1",
		"2",
		"3",
	}
	rows := []model.TraceID{
		{High: 0, Low: 1},
		{High: 0, Low: 2},
		{High: 0, Low: 3},
	}

	tests := map[string]struct {
		queryParams   spanstore.TraceQueryParameters
		skip          []model.TraceID
		expectedQuery string
		expectedArgs  []driver.Value
	}{
		"default": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces},
			skip:        make([]model.TraceID, 0),
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
			),
			expectedArgs: []driver.Value{
				service,
				start,
				end,
				testNumTraces,
			},
		},
		"maxDuration": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces, DurationMax: maxDuration},
			skip:        make([]model.TraceID, 0),
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? AND durationUs <= ? ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
			),
			expectedArgs: []driver.Value{
				service,
				start,
				end,
				maxDuration.Microseconds(),
				testNumTraces,
			},
		},
		"minDuration": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces, DurationMin: minDuration},
			skip:        make([]model.TraceID, 0),
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? AND durationUs >= ? ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
			),
			expectedArgs: []driver.Value{
				service,
				start,
				end,
				minDuration.Microseconds(),
				testNumTraces,
			},
		},
		"tags": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces, Tags: tags},
			skip:        make([]model.TraceID, 0),
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ?%s ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
				strings.Repeat(" AND has(tags, ?)", len(tags)),
			),
			expectedArgs: []driver.Value{
				service,
				start,
				end,
				tagArgs[0],
				testNumTraces,
			},
		},
		"skip": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces},
			skip:        skip,
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? AND traceID NOT IN (?,?) ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
			),
			expectedArgs: []driver.Value{
				service,
				start,
				end,
				skip[0].String(),
				skip[1].String(),
				testNumTraces - len(skip),
			},
		},
		"operation": {
			queryParams: spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces, OperationName: operation},
			skip:        make([]model.TraceID, 0),
			expectedQuery: fmt.Sprintf(
				"SELECT DISTINCT traceID FROM %s WHERE service = ? AND operation = ? AND timestamp >= ? AND timestamp <= ? ORDER BY service, timestamp DESC LIMIT ?",
				testIndexTable,
			),
			expectedArgs: []driver.Value{
				service,
				operation,
				start,
				end,
				testNumTraces,
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			queryResult := sqlmock.NewRows([]string{"traceID"})
			for _, row := range rowValues {
				queryResult.AddRow(row)
			}

			mock.
				ExpectQuery(test.expectedQuery).
				WithArgs(test.expectedArgs...).
				WillReturnRows(queryResult)

			res, err := traceReader.findTraceIDsInRange(
				context.Background(),
				&test.queryParams,
				start,
				end,
				test.skip)
			require.NoError(t, err)
			assert.Equal(t, rows, res)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

func TestSpanReader_findTraceIDsInRangeNoIndexTable(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, "", testSpansTable)
	res, err := traceReader.findTraceIDsInRange(
		context.Background(),
		nil,
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
		make([]model.TraceID, 0),
	)
	assert.Equal(t, []model.TraceID(nil), res)
	assert.EqualError(t, err, errNoIndexTable.Error())
}

func TestSpanReader_findTraceIDsInRangeEndBeforeStart(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)
	res, err := traceReader.findTraceIDsInRange(
		context.Background(),
		nil,
		time.Date(2000, 1, 2, 0, 0, 0, 0, time.UTC),
		time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC),
		make([]model.TraceID, 0),
	)
	assert.Equal(t, make([]model.TraceID, 0), res)
	assert.NoError(t, err)
}

func TestSpanReader_findTraceIDsInRangeQueryError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)
	service := "test_service"
	start := time.Unix(0, 0)
	end := time.Now()

	mock.
		ExpectQuery(fmt.Sprintf(
			"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? ORDER BY service, timestamp DESC LIMIT ?",
			testIndexTable,
		)).
		WithArgs(
			service,
			start,
			end,
			testNumTraces,
		).
		WillReturnError(errorMock)

	res, err := traceReader.findTraceIDsInRange(
		context.Background(),
		&spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces},
		start,
		end,
		make([]model.TraceID, 0))
	assert.EqualError(t, err, errorMock.Error())
	assert.Equal(t, []model.TraceID(nil), res)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSpanReader_findTraceIDsInRangeIncorrectData(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)
	service := "test_service"
	start := time.Unix(0, 0)
	end := time.Now()
	rowValues := []driver.Value{
		"1",
		"incorrect value",
		"3",
	}
	queryResult := sqlmock.NewRows([]string{"traceID"})
	for _, row := range rowValues {
		queryResult.AddRow(row)
	}

	mock.
		ExpectQuery(fmt.Sprintf(
			"SELECT DISTINCT traceID FROM %s WHERE service = ? AND timestamp >= ? AND timestamp <= ? ORDER BY service, timestamp DESC LIMIT ?",
			testIndexTable,
		)).
		WithArgs(
			service,
			start,
			end,
			testNumTraces,
		).
		WillReturnRows(queryResult)

	res, err := traceReader.findTraceIDsInRange(
		context.Background(),
		&spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces},
		start,
		end,
		make([]model.TraceID, 0))
	assert.Error(t, err)
	assert.Equal(t, []model.TraceID(nil), res)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestSpanReader_getStrings(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	query := "SELECT b FROM a WHERE b != ?"
	argValues := []driver.Value{driver.Value("a")}
	args := []interface{}{"a"}
	rows := []driver.Value{"some", "query", "rows"}
	expectedResult := []string{"some", "query", "rows"}
	result := sqlmock.NewRows([]string{"b"})
	for _, str := range rows {
		result.AddRow(str)
	}
	mock.ExpectQuery(query).WithArgs(argValues...).WillReturnRows(result)

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)

	queryResult, err := traceReader.getStrings(context.Background(), query, args...)
	assert.NoError(t, err)
	assert.EqualValues(t, expectedResult, queryResult)
}

func TestSpanReader_getStringsQueryError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	query := "SELECT b FROM a WHERE b != ?"
	argValues := []driver.Value{driver.Value("a")}
	args := []interface{}{"a"}
	mock.ExpectQuery(query).WithArgs(argValues...).WillReturnError(errorMock)

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)

	queryResult, err := traceReader.getStrings(context.Background(), query, args...)
	assert.EqualError(t, err, errorMock.Error())
	assert.EqualValues(t, []string(nil), queryResult)
}

func TestSpanReader_getStringsRowError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	query := "SELECT b FROM a WHERE b != ?"
	argValues := []driver.Value{driver.Value("a")}
	args := []interface{}{"a"}
	rows := []driver.Value{"some", "query", "rows"}
	result := sqlmock.NewRows([]string{"b"})
	for _, str := range rows {
		result.AddRow(str)
	}
	result.RowError(2, errorMock)
	mock.ExpectQuery(query).WithArgs(argValues...).WillReturnRows(result)

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)

	queryResult, err := traceReader.getStrings(context.Background(), query, args...)
	assert.EqualError(t, err, errorMock.Error())
	assert.EqualValues(t, []string(nil), queryResult)
}
