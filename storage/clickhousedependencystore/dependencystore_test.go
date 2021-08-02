package clickhousedependencystore

import (
	"context"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestDependencyStore_GetDependencies(t *testing.T) {
	dependencyStore := DependencyStore{}

	dependencies, err := dependencyStore.GetDependencies(context.Background(), time.Now(), time.Hour)

	assert.EqualError(t, err, errNotImplemented.Error())
	assert.Nil(t, dependencies)
}
