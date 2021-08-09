package clickhousespanstore

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTableName_AddDbName(t *testing.T) {
	assert.Equal(t, TableName("database_name.table_name_local"), TableName("table_name_local").AddDbName("database_name"))
}

func TestTableName_ToGlobal(t *testing.T) {
	tests := map[string]struct {
		tableName TableName
		expected  TableName
	}{
		"trailing '_local'": {tableName: "table_name_local", expected: "table_name"},
		"internal '_local'": {tableName: "table_name_local_suffix", expected: "table_name_suffix"},
		"no '_local'":       {tableName: "table_name", expected: "table_name"},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.expected, test.tableName.ToGlobal())
		})
	}
}
