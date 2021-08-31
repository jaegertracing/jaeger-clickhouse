package mocks

import (
	"database/sql"

	"github.com/DATA-DOG/go-sqlmock"
)

func GetDbMock() (*sql.DB, sqlmock.Sqlmock, error) {
	return sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual),
		sqlmock.ValueConverterOption(ConverterMock{}),
	)
}
