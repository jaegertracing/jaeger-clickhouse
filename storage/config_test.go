package storage

import (
	"fmt"
	"math/rand"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDefaults(t *testing.T) {
	config := Configuration{}
	config.setDefaults()
	tests := map[string]struct {
		field    interface{}
		expected interface{}
	}{
		"username":              {field: config.Username, expected: defaultUsername},
		"database name":         {field: config.Database, expected: defaultDatabaseName},
		"encoding":              {field: config.Encoding, expected: defaultEncoding},
		"batch write size":      {field: config.BatchWriteSize, expected: defaultBatchSize},
		"batch flush interval":  {field: config.BatchFlushInterval, expected: defaultBatchDelay},
		"metrics endpoint":      {field: config.MetricsEndpoint, expected: defaultMetricsEndpoint},
		"spans table name":      {field: config.SpansTable, expected: defaultSpansTable},
		"index table name":      {field: config.SpansIndexTable, expected: defaultSpansIndexTable},
		"operations table name": {field: config.OperationsTable, expected: defaultOperationsTable},
	}

	for name, test := range tests {
		t.Run(fmt.Sprintf("default %s", name), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.field)
		})
	}
}

func TestConfiguration_GetSpansArchiveTable(t *testing.T) {
	const repetitionCount = 100
	defaultConfig := Configuration{}
	defaultConfig.setDefaults()
	assert.Equal(t, defaultSpansTable + "_archive", defaultConfig.getSpansArchiveTable())
	for i := 0; i < 100; i++ {
		tableName := "table_" + strconv.FormatUint(rand.Uint64(), 16)
		config := Configuration{SpansTable: tableName}
		assert.Equal(t, tableName + "_archive", config.getSpansArchiveTable())
	}
}
