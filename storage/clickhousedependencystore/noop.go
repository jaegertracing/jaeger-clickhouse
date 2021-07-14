package clickhousedependencystore

import (
	"context"
	"errors"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
)

var (
	errNotImplemented = errors.New("not implemented")
)

// DependencyStore handles all queries and insertions to Clickhouse dependencies
type DependencyStore struct {
}

var _ dependencystore.Reader = (*DependencyStore)(nil)

// NewDependencyStore returns a DependencyStore
func NewDependencyStore() *DependencyStore {
	return &DependencyStore{}
}

// GetDependencies returns all interservice dependencies, implements DependencyReader
func (s *DependencyStore) GetDependencies(_ context.Context, _ time.Time, _ time.Duration) ([]model.DependencyLink, error) {
	return nil, errNotImplemented
}
