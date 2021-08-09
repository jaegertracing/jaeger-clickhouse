package storage

import (
	"fmt"
	"testing"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"

	"github.com/stretchr/testify/assert"
)

func TestSetDefaults(t *testing.T) {
	tests := map[string]struct {
		getField func(Configuration) interface{}
		expected interface{}
	}{
		"username":              {
			getField: func(config Configuration) interface{} { return config.Username },
			expected: defaultUsername,
		},
		"database name":         {
			getField: func(config Configuration) interface{} { return config.Database },
			expected: defaultDatabaseName,
		},
		"encoding":              {
			getField: func(config Configuration) interface{} { return config.Encoding },
			expected: defaultEncoding,
		},
		"batch write size":      {
			getField: func(config Configuration) interface{} { return config.BatchWriteSize },
			expected: defaultBatchSize,
		},
		"batch flush interval":  {
			getField: func(config Configuration) interface{} { return config.BatchFlushInterval },
			expected: defaultBatchDelay,
		},
		"metrics endpoint":      {
			getField: func(config Configuration) interface{} { return config.MetricsEndpoint },
			expected: defaultMetricsEndpoint,
		},
		"spans table name ":     {
			getField: func(config Configuration) interface{} { return config.SpansTable },
			expected: defaultSpansTable.ToLocal(),
		},
		"index table name":      {
			getField: func(config Configuration) interface{} { return config.SpansIndexTable },
			expected: defaultSpansIndexTable.ToLocal(),
		},
		"operations table name": {
			getField: func(config Configuration) interface{} { return config.OperationsTable },
			expected: defaultOperationsTable.ToLocal(),
		},
	}

	for name, test := range tests {
		t.Run(fmt.Sprintf("default %s", name), func(t *testing.T) {
			config := Configuration{}
			config.setDefaults()
			assert.EqualValues(t, test.expected, test.getField(config))
		})
	}
}

func TestConfiguration_GetSpansArchiveTable(t *testing.T) {
	tests := map[string]struct {
		config                        Configuration
		expectedSpansArchiveTableName clickhousespanstore.TableName
	}{
		"default_config_local":       {config: Configuration{}, expectedSpansArchiveTableName: (defaultSpansTable + "_archive").ToLocal()},
		"custom_spans_table": {config: Configuration{SpansTable: "custom_table_name"}, expectedSpansArchiveTableName: "custom_table_name_archive"},
		"custom_spans_table":         {config: Configuration{SpansTable: "custom_table_name"}, expectedSpansArchiveTableName: "custom_table_name_archive"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			test.config.setDefaults()
			assert.Equal(t, test.expectedSpansArchiveTableName, test.config.GetSpansArchiveTable())
		})
	}
}
