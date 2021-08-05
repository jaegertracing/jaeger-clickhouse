package clickhousespanstore

import (
	"fmt"
	"strings"
)

type TableName string

func (tableName TableName) ToGlobal() TableName {
	return TableName(strings.ReplaceAll(string(tableName), "_local", ""))
}

func (tableName TableName) AddDbName(databaseName string) TableName {
	return TableName(fmt.Sprintf("%s.%s", databaseName, tableName))
}
