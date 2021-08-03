package storage

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetDefaults(t *testing.T) {
	config := Configuration{}
	config.setDefaults()

	assert.Equal(t, defaultUsername, config.Username, "Incorrect default username")
	assert.Equal(t, defaultDatabaseName, config.Database, "Incorrect default database name")
	assert.Equal(t, defaultEncoding, config.Encoding, "Incorrect default encoding")
	assert.EqualValues(t, defaultBatchSize, config.BatchWriteSize, "Incorrect default batch write size")
	assert.Equal(t, defaultBatchDelay, config.BatchFlushInterval, "Incorrect default batch flush interval")
	assert.Equal(t, defaultMetricsEndpoint, config.MetricsEndpoint, "Incorrect default metrics endpoint")
	assert.Equal(t, defaultSpansTable, config.SpansTable, "Incorrect default spans table name")
	assert.Equal(t, defaultSpansIndexTable, config.SpansIndexTable, "Incorrect default index table name")
	assert.Equal(t, defaultOperationsTable, config.OperationsTable, "Incorrect default operations table name")
}
