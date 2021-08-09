package storage

import (
	"fmt"
	"testing"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"

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
	tests := map[string]struct {
		config                        Configuration
		expectedSpansArchiveTableName clickhousespanstore.TableName
	}{
		"default_config":     {config: Configuration{}, expectedSpansArchiveTableName: defaultSpansTable + "_archive"},
		"custom_spans_table": {config: Configuration{SpansTable: "custom_table_name"}, expectedSpansArchiveTableName: "custom_table_name_archive"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			test.config.setDefaults()
			assert.Equal(t, test.expectedSpansArchiveTableName, test.config.GetSpansArchiveTable())
		})
	}
}
