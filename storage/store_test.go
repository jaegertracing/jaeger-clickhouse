package storage

import (
	"database/sql"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/hashicorp/go-hclog"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

var errorMock = fmt.Errorf("error mock")

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
	assert.NoError(t, err)
	//assert.EqualError(t, err, errorMock.Error())
}

func getDbMock() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(mocks.ConverterMock{}),
	)
}
