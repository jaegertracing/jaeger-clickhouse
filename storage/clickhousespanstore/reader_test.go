package clickhousespanstore

import (
	"context"
	"database/sql/driver"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

const (
	testOperationsTable = "test_operations_table"
	testNumTraces       = 10
)

func TestSpanReader_findTraceIDsInRangeDefault(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	traceReader := NewTraceReader(db, testOperationsTable, testIndexTable, testSpansTable)

	service := "test_service"
	params := spanstore.TraceQueryParameters{ServiceName: service, NumTraces: testNumTraces}
	start := time.Unix(0, 0)
	end := time.Now()
	queryResult := sqlmock.NewRows([]string{"traceID"})
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
		&params,
		start,
		end,
		make([]model.TraceID, 0))
	require.NoError(t, err)
	assert.Equal(t, rows, res)
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
