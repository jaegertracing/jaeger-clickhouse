package storage

import (
	"database/sql"
	"embed"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"

	_ "github.com/ClickHouse/clickhouse-go" // force SQL driver registration
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"
)

const (
	defaultBatchSize  = 10_000
	defaultBatchDelay = time.Second * 5
)

type Store struct {
	db     *sql.DB
	logger hclog.Logger
	cfg    Configuration
}

var _ shared.StoragePlugin = (*Store)(nil)
var _ shared.ArchiveStoragePlugin = (*Store)(nil)
var _ io.Closer = (*Store)(nil)

func NewStore(logger hclog.Logger, cfg Configuration, embeddedSQLScripts embed.FS) (*Store, error) {
	db, err := defaultConnector(cfg.Address)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := initializeDB(db, cfg.InitSQLScriptsDir, embeddedSQLScripts); err != nil {
		db.Close()
		return nil, err
	}
	if cfg.BatchWriteSize == 0 {
		cfg.BatchWriteSize = defaultBatchSize
	}
	if cfg.BatchFlushInterval == 0 {
		cfg.BatchFlushInterval = defaultBatchDelay
	}
	if cfg.Encoding == "" {
		cfg.Encoding = JsonEncoding
	}
	return &Store{
		db:     db,
		logger: logger,
		cfg:    cfg,
	}, nil
}

func initializeDB(db *sql.DB, initSQLScriptsDir string, embeddedScripts embed.FS) error {
	var sqlStatements []string
	if initSQLScriptsDir != "" {
		filePaths, err := walkMatch(initSQLScriptsDir, "*.sql")
		if err != nil {
			return fmt.Errorf("could not list sql files: %q", err)
		}
		sort.Strings(filePaths)
		for _, f := range filePaths {
			sqlStatement, err := ioutil.ReadFile(f)
			if err != nil {
				return err
			}
			sqlStatements = append(sqlStatements, string(sqlStatement))
		}
	} else {
		f, err := embeddedScripts.ReadFile("sqlscripts/0001-jaeger-index.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
		f, err = embeddedScripts.ReadFile("sqlscripts/0002-jaeger-spans.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
		f, err = embeddedScripts.ReadFile("sqlscripts/0003-jaeger-operations.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
		f, err = embeddedScripts.ReadFile("sqlscripts/0004-jaeger-spans-archive.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
	}
	return executeScripts(sqlStatements, db)
}

func (s *Store) SpanReader() spanstore.Reader {
	return clickhousespanstore.NewTraceReader(s.db, "jaeger_operations_v2", "jaeger_index_v2", "jaeger_spans_v2")
}

func (s *Store) SpanWriter() spanstore.Writer {
	return clickhousespanstore.NewSpanWriter(s.logger, s.db, "jaeger_index_v2", "jaeger_spans_v2", clickhousespanstore.Encoding(s.cfg.Encoding), s.cfg.BatchFlushInterval, s.cfg.BatchWriteSize)
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return clickhousedependencystore.NewDependencyStore()
}

func (s *Store) ArchiveSpanReader() spanstore.Reader {
	return clickhousespanstore.NewTraceReader(s.db, "", "", "jaeger_archive_spans_v2")
}

func (s *Store) ArchiveSpanWriter() spanstore.Writer {
	return clickhousespanstore.NewSpanWriter(s.logger, s.db, "", "jaeger_archive_spans_v2", clickhousespanstore.Encoding(s.cfg.Encoding), s.cfg.BatchFlushInterval, s.cfg.BatchWriteSize)
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

func executeScripts(sqlStatements []string, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return nil
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	for _, file := range sqlStatements {
		_, err = tx.Exec(file)
		if err != nil {
			return fmt.Errorf("could not run sql %q: %q", file, err)
		}
	}
	committed = true
	return tx.Commit()
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
