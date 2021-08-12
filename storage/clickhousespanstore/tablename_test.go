package clickhousespanstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableName_AddDbName(t *testing.T) {
	assert.Equal(t, TableName("database_name.table_name_local"), TableName("table_name_local").AddDbName("database_name"))
}

func TestTableName_ToLocal(t *testing.T) {
	tableName := TableName("some_table")
	assert.Equal(t, tableName+"_local", tableName.ToLocal())

}
