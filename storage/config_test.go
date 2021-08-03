package storage

import (
	"fmt"
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
		"username":              {config.Username, defaultUsername},
		"database name":         {config.Database, defaultDatabaseName},
		"encoding":              {config.Encoding, defaultEncoding},
		"batch write size":      {config.BatchWriteSize, defaultBatchSize},
		"batch flush interval":  {config.BatchFlushInterval, defaultBatchDelay},
		"metrics endpoint":      {config.MetricsEndpoint, defaultMetricsEndpoint},
		"spans table name":      {config.SpansTable, defaultSpansTable},
		"index table name":      {config.SpansIndexTable, defaultSpansIndexTable},
		"operations table name": {config.OperationsTable, defaultOperationsTable},
	}

	for name, test := range tests {
		t.Run(fmt.Sprintf("default %s", name), func(t *testing.T) {
			assert.EqualValues(t, test.expected, test.field)
		})
	}
}
