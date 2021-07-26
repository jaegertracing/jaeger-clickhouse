package storage

import "testing"

func TestSetDefaults(t *testing.T) {
	config := Configuration{}
	config.setDefaults()

	if config.Username != defaultUsername {
		t.Errorf("Default username is \"%s\", want \"%s\"", config.Database, defaultUsername)
	}
	if config.Database != defaultDatabaseName {
		t.Errorf("Default database name is \"%s\", want \"%s\"", config.Database, defaultDatabaseName)
	}
	if config.Encoding != defaultEncoding {
		t.Errorf("Default encoding is \"%s\", want \"%s\"", config.Encoding, defaultEncoding)
	}
	if config.BatchWriteSize != defaultBatchSize {
		t.Errorf("Default batch write size is \"%d\", want \"%d\"", config.BatchWriteSize, defaultBatchSize)
	}
	if config.BatchFlushInterval != defaultBatchDelay {
		t.Errorf("Default batch flush size is \"%d\", want \"%d\"", config.BatchFlushInterval, defaultBatchDelay)
	}
	if config.MetricsEndpoint != defaultMetricsEndpoint {
		t.Errorf("Default metrics endpoint is \"%s\", want \"%s\"", config.MetricsEndpoint, defaultMetricsEndpoint)
	}
}
