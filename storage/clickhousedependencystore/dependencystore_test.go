package clickhousedependencystore

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestDependencyStore_GetDependencies(t *testing.T) {
	dependencyStore := DependencyStore{}

	dependencies, err := dependencyStore.GetDependencies(context.TODO(), time.Now(), time.Hour)

	if err != errNotImplemented {
		t.Errorf("Expected GetDependencies not to be implemented, got %s", err)
	}

	if dependencies != nil {
		t.Error(fmt.Sprint("Expected GetDependencies result to be nil, got", dependencies))
	}
}
