package storage

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSetDefaults(t *testing.T) {
	config := Configuration{}
	config.setDefaults()

	assert.Equal(t, config.Username, defaultUsername, "Default username is \"%s\", want \"%s\"", config.Database, defaultUsername)
	assert.Equal(t, config.Database, defaultDatabaseName, "Default database name is \"%s\", want \"%s\"", config.Database, defaultDatabaseName)
	assert.Equal(t, config.Encoding, defaultEncoding, "Default encoding is \"%s\", want \"%s\"", config.Encoding, defaultEncoding)
	assert.EqualValues(t, config.BatchWriteSize, defaultBatchSize, "Default batch write size is \"%d\", want \"%d\"", config.BatchWriteSize, defaultBatchSize)
	assert.Equal(t, config.BatchFlushInterval, defaultBatchDelay, "Default batch flush size is \"%d\", want \"%d\"", config.BatchFlushInterval, defaultBatchDelay)
	assert.Equal(t, config.MetricsEndpoint, defaultMetricsEndpoint, "Default metrics endpoint is \"%s\", want \"%s\"", config.MetricsEndpoint, defaultMetricsEndpoint)
	assert.Equal(t, config.SpansTable, defaultSpansTable, "Default spans table name is is \"%s\", want \"%s\"", config.SpansTable, defaultSpansTable)
	assert.Equal(t, config.SpansIndexTable, defaultSpansIndexTable, "Default index table name is is \"%s\", want \"%s\"", config.SpansIndexTable, defaultSpansIndexTable)
	assert.Equal(t, config.OperationsTable, defaultOperationsTable, "Default operations table name is is \"%s\", want \"%s\"", config.OperationsTable, defaultOperationsTable)
}
