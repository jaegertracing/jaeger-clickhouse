package storage

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hashicorp/go-hclog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
)

const (
	testIndexTable        = "test_index_table"
	testSpansTable        = "test_spans_table"
	testOperationsTable   = "test_operation_table"
	testSpansArchiveTable = "test_spans_archive_table"
)

var errorMock = fmt.Errorf("error mock")

func TestStore_SpanWriter(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	writer := clickhousespanstore.SpanWriter{}
	store := Store{
		db:     db,
		writer: &writer,
	}
	assert.Equal(t, &writer, store.SpanWriter())
}

func TestStore_ArchiveSpanWriter(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	writer := clickhousespanstore.SpanWriter{}
	store := Store{
		db:            db,
		archiveWriter: &writer,
	}
	assert.Equal(t, &writer, store.ArchiveSpanWriter())
}

func TestStore_SpanReader(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	reader := clickhousespanstore.TraceReader{}
	store := Store{
		db:     db,
		reader: &reader,
	}
	assert.Equal(t, &reader, store.SpanReader())
}

func TestStore_ArchiveSpanReader(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	reader := clickhousespanstore.TraceReader{}
	store := Store{
		db:            db,
		archiveReader: &reader,
	}
	assert.Equal(t, &reader, store.ArchiveSpanReader())
}

func TestStore_DependencyReader(t *testing.T) {
	db, _, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	logger := mocks.NewSpyLogger()
	store := newStore(db, logger)
	assert.Equal(t, &clickhousedependencystore.DependencyStore{}, store.DependencyReader())
	logger.AssertLogsEmpty(t)
}

func TestStore_Close(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	logger := mocks.NewSpyLogger()
	store := newStore(db, logger)

	mock.ExpectClose()
	require.NoError(t, store.Close())
	assert.NoError(t, mock.ExpectationsWereMet())
	logger.AssertLogsEmpty(t)
}

func newStore(db *sql.DB, logger mocks.SpyLogger) Store {
	return Store{
		db: db,
		writer: clickhousespanstore.NewSpanWriter(
			logger,
			db,
			testIndexTable,
			testSpansTable,
			clickhousespanstore.EncodingJSON,
			0,
			0,
		),
		reader: clickhousespanstore.NewTraceReader(
			db,
			testOperationsTable,
			testIndexTable,
			testSpansTable,
		),
		archiveWriter: clickhousespanstore.NewSpanWriter(
			logger,
			db,
			testIndexTable,
			testSpansArchiveTable,
			clickhousespanstore.EncodingJSON,
			0,
			0,
		),
		archiveReader: clickhousespanstore.NewTraceReader(
			db,
			testOperationsTable,
			testIndexTable,
			testSpansArchiveTable,
		),
	}
}

func TestStore_executeScripts(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	scripts := []string{
		"first SQL script",
		"second_SQL_script",
	}

	mock.ExpectBegin()
	for _, script := range scripts {
		mock.ExpectExec(script).WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()
	err = executeScripts(spyLogger, scripts, db)
	require.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
	spyLogger.AssertLogsOfLevelEqual(t, hclog.Debug, func() []mocks.LogMock {
		res := make([]mocks.LogMock, len(scripts))
		for i, script := range scripts {
			res[i] = mocks.LogMock{Msg: "Running SQL statement", Args: []interface{}{"statement", script}}
		}
		return res
	}())
}

func TestStore_executeScriptsExecuteError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	scripts := []string{
		"first SQL script",
		"second_SQL_script",
	}

	mock.ExpectBegin()
	mock.ExpectExec(scripts[0]).WillReturnError(errorMock)
	mock.ExpectRollback()
	err = executeScripts(spyLogger, scripts, db)
	assert.EqualError(t, err, fmt.Sprintf("could not run sql %q: %q", scripts[0], errorMock))
	spyLogger.AssertLogsOfLevelEqual(
		t,
		hclog.Debug,
		[]mocks.LogMock{{Msg: "Running SQL statement", Args: []interface{}{"statement", scripts[0]}}},
	)
}

func TestStore_executeScriptBeginError(t *testing.T) {
	db, mock, err := getDbMock()
	require.NoError(t, err, "an error was not expected when opening a stub database connection")
	defer db.Close()

	spyLogger := mocks.NewSpyLogger()
	scripts := []string{
		"first SQL script",
		"second_SQL_script",
	}

	mock.ExpectBegin().WillReturnError(errorMock)
	err = executeScripts(spyLogger, scripts, db)
	assert.EqualError(t, err, errorMock.Error())
}

func getDbMock() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(mocks.ConverterMock{}),
	)
}
