package clickhousespanstore

import (
	"fmt"
)

type TableName string

func (tableName TableName) ToLocal() TableName {
	return tableName + "_local"
}

func (tableName TableName) AddDbName(databaseName string) TableName {
	return TableName(fmt.Sprintf("%s.%s", databaseName, tableName))
}
