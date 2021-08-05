package clickhousespanstore

import "fmt"

const databaseName = "jaeger"

type TableName string

func (tableName TableName) ToGlobal() TableName {
	return tableName[:len(tableName)-6]
}

func (tableName TableName) AddDbName() TableName {
	return TableName(fmt.Sprintf("%s_%s", databaseName, tableName))
}
