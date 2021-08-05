package clickhousespanstore

import (
	"fmt"
	"strings"
)

const databaseName = "jaeger"

type TableName string

func (tableName TableName) ToGlobal() TableName {
	return TableName(strings.ReplaceAll(string(tableName), "_local", ""))
}

func (tableName TableName) AddDbName() TableName {
	return TableName(fmt.Sprintf("%s.%s", databaseName, tableName))
}
