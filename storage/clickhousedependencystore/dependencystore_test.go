package clickhousedependencystore

import (
	"testing"
	"time"
)

func TestDependencyStore_GetDependencies(t *testing.T) {
	dependencyStore := DependencyStore{}

	if _, err := dependencyStore.GetDependencies(nil, time.Now(), time.Hour); err != errNotImplemented {
		t.Errorf("Expected GetDependencies not to be implemented, got %s", err)
	}
}
