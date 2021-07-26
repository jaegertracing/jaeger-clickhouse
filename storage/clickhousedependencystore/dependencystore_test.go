package clickhousedependencystore

import (
	"context"
	"testing"
	"time"
)

func TestDependencyStore_GetDependencies(t *testing.T) {
	dependencyStore := DependencyStore{}

	if _, err := dependencyStore.GetDependencies(context.TODO(), time.Now(), time.Hour); err != errNotImplemented {
		t.Errorf("Expected GetDependencies not to be implemented, got %s", err)
	}
}
