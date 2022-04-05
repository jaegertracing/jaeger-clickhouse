package clickhousespanstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableName_ToLocal(t *testing.T) {
	tableName := TableName("some_table")
	assert.Equal(t, tableName+"_local", tableName.ToLocal())

}
