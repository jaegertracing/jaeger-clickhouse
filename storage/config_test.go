package storage

import (
	"fmt"
	"testing"

	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousespanstore"

	"github.com/stretchr/testify/assert"
)

func TestSetDefaults(t *testing.T) {
	tests := map[string]struct {
		replication bool
		getField    func(Configuration) interface{}
		expected    interface{}
	}{
		"username": {
			getField: func(config Configuration) interface{} { return config.Username },
			expected: defaultUsername,
		},
		"database name": {
			getField: func(config Configuration) interface{} { return config.Database },
			expected: defaultDatabaseName,
		},
		"encoding": {
			getField: func(config Configuration) interface{} { return config.Encoding },
			expected: defaultEncoding,
		},
		"batch write size": {
			getField: func(config Configuration) interface{} { return config.BatchWriteSize },
			expected: defaultBatchSize,
		},
		"batch flush interval": {
			getField: func(config Configuration) interface{} { return config.BatchFlushInterval },
			expected: defaultBatchDelay,
		},
		"max span count": {
			getField: func(config Configuration) interface{} { return config.MaxSpanCount },
			expected: defaultMaxSpanCount,
		},
		"metrics endpoint": {
			getField: func(config Configuration) interface{} { return config.MetricsEndpoint },
			expected: defaultMetricsEndpoint,
		},
		"spans table name local": {
			getField: func(config Configuration) interface{} { return config.SpansTable },
			expected: defaultSpansTable.ToLocal(),
		},
		"spans table name replication": {
			replication: true,
			getField:    func(config Configuration) interface{} { return config.SpansTable },
			expected:    defaultSpansTable,
		},
		"index table name local": {
			getField: func(config Configuration) interface{} { return config.SpansIndexTable },
			expected: defaultSpansIndexTable.ToLocal(),
		},
		"index table name replication": {
			replication: true,
			getField:    func(config Configuration) interface{} { return config.SpansIndexTable },
			expected:    defaultSpansIndexTable,
		},
		"operations table name local": {
			getField: func(config Configuration) interface{} { return config.OperationsTable },
			expected: defaultOperationsTable.ToLocal(),
		},
		"operations table name replication": {
			replication: true,
			getField:    func(config Configuration) interface{} { return config.OperationsTable },
			expected:    defaultOperationsTable,
		},
		"max number spans": {
			getField: func(config Configuration) interface{} { return config.MaxNumSpans },
			expected: defaultMaxNumSpans,
		},
	}

	for name, test := range tests {
		t.Run(fmt.Sprintf("default %s", name), func(t *testing.T) {
			config := Configuration{Replication: test.replication}
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
		"default_config_replication": {config: Configuration{Replication: true}, expectedSpansArchiveTableName: defaultSpansTable + "_archive"},
		"custom_spans_table":         {config: Configuration{SpansTable: "custom_table_name"}, expectedSpansArchiveTableName: "custom_table_name_archive"},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			test.config.setDefaults()
			assert.Equal(t, test.expectedSpansArchiveTableName, test.config.GetSpansArchiveTable())
		})
	}
}

func TestConfiguration_InitTables(test *testing.T) {
	// for pointers below
	t := true
	f := false
	tests := map[string]struct {
		config             Configuration
		expectedInitTables bool
	}{
		"scriptsempty_initnil":      {config: Configuration{}, expectedInitTables: true},
		"scriptsprovided_initnil":   {config: Configuration{InitSQLScriptsDir: "hello"}, expectedInitTables: false},
		"scriptsempty_inittrue":     {config: Configuration{InitTables: &t}, expectedInitTables: true},
		"scriptsprovided_inittrue":  {config: Configuration{InitSQLScriptsDir: "hello", InitTables: &t}, expectedInitTables: true},
		"scriptsempty_initfalse":    {config: Configuration{InitTables: &f}, expectedInitTables: false},
		"scriptsprovided_initfalse": {config: Configuration{InitSQLScriptsDir: "hello", InitTables: &f}, expectedInitTables: false},
	}

	for name, testcase := range tests {
		test.Run(name, func(t *testing.T) {
			testcase.config.setDefaults()
			assert.Equal(t, testcase.expectedInitTables, *(testcase.config.InitTables))
		})
	}
}
