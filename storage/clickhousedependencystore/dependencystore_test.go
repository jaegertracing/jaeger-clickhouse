package clickhousedependencystore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDependencyStore_GetDependencies(t *testing.T) {
	dependencyStore := NewDependencyStore()

	dependencies, err := dependencyStore.GetDependencies(context.Background(), time.Now(), time.Hour)

	assert.EqualError(t, err, errNotImplemented.Error())
	assert.Nil(t, dependencies)
}
