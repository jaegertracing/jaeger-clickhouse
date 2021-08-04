package storage

import "fmt"

const databaseName = "jaeger"

type TableName string

func (tableName TableName) toGlobal() TableName {
	return tableName[:len(tableName)-6]
}

func (tableName TableName) addDbName() TableName {
	return TableName(fmt.Sprintf("%s_%s", databaseName, tableName))
}
