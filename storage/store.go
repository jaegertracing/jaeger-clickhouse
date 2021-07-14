package storage

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"database/sql"
	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"
)

type Store struct {
	db *sql.DB
	logger hclog.Logger
}

var _ shared.StoragePlugin = (*Store)(nil)
var _ io.Closer = (*Store)(nil)

func NewStore(logger hclog.Logger) (*Store, error) {
	sqlFiles, err := walkMatch("/home/ploffay/projects/jaegertracing/jaeger-clickhouse", "*.sql")
	if err != nil {
		return nil, fmt.Errorf("could not list sql files: %q", err)
	}
	db, err := defaultConnector("tcp://localhost:9000")
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}
	for _, file := range sqlFiles {
		logger.Info(file)
		sqlStatement, err := ioutil.ReadFile(file)
		if err != nil {
			db.Close()
			return nil, err
		}
		_, err = db.Exec(string(sqlStatement))
		if err != nil {
			db.Close()
			return nil, fmt.Errorf("could not run sql %q: %q", file, err)
		}
	}
	return &Store{
		db: db,
		logger: logger,
	}, nil
}


func (s *Store) SpanReader() spanstore.Reader {
	return clickhousespanstore.NewTraceReader(s.db, "jaeger_operations_v2", "jaeger_index_v2", "jaeger_spans_v2")
}

func (s *Store) SpanWriter() spanstore.Writer {
	return clickhousespanstore.NewSpanWriter(s.logger, s.db, "jaeger_index_v2", "jaeger_spans_v2", clickhousespanstore.EncodingJSON, 5 * time.Second, 10000)
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return clickhousedependencystore.NewDependencyStore()
}

func (s *Store) Close() error {
	return s.db.Close()
}

func defaultConnector(datasource string) (*sql.DB, error) {
	db, err := sql.Open("clickhouse", datasource)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}


func walkMatch(root, pattern string) ([]string, error) {
	var matches []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if matched, err := filepath.Match(pattern, filepath.Base(path)); err != nil {
			return err
		} else if matched {
			matches = append(matches, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return matches, nil
}
