package storage

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"embed"
	"fmt"
	"github.com/ClickHouse/clickhouse-go"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"
)

const (
	defaultBatchSize  = 10_000
	defaultBatchDelay = time.Second * 5
	tlsConfigKey      = "clickhouse_tls_config_key"
)

type Store struct {
	db            *sql.DB
	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
}

var _ shared.StoragePlugin = (*Store)(nil)
var _ shared.ArchiveStoragePlugin = (*Store)(nil)
var _ io.Closer = (*Store)(nil)

func NewStore(logger hclog.Logger, cfg Configuration, embeddedSQLScripts embed.FS) (*Store, error) {
	cfg.setDefaults()
	var db *sql.DB
	var err error
	db, err = connector(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := initializeDB(db, cfg.InitSQLScriptsDir, embeddedSQLScripts); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{
		db:            db,
		writer:        clickhousespanstore.NewSpanWriter(logger, db, "jaeger_index_v2", "jaeger_spans_v2", clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		reader:        clickhousespanstore.NewTraceReader(logger, db, "jaeger_operations_v2", "jaeger_index_v2", "jaeger_spans_v2"),
		archiveWriter: clickhousespanstore.NewSpanWriter(logger, db, "", "jaeger_archive_spans_v2", clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		archiveReader: clickhousespanstore.NewTraceReader(logger, db, "", "", "jaeger_archive_spans_v2"),
	}, nil
}

func connector(cfg Configuration) (*sql.DB, error) {
	if cfg.TLSConnection {
		return tlsConnector(cfg)
	}
	return defaultConnector(cfg)
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
			sqlStatement, err := ioutil.ReadFile(filepath.Clean(f))
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
	return s.reader
}

func (s *Store) SpanWriter() spanstore.Writer {
	return s.writer
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return clickhousedependencystore.NewDependencyStore()
}

func (s *Store) ArchiveSpanReader() spanstore.Reader {
	return s.archiveReader
}

func (s *Store) ArchiveSpanWriter() spanstore.Writer {
	return s.archiveWriter
}

func (s *Store) Close() error {
	return s.db.Close()
}

func defaultConnector(cfg Configuration) (*sql.DB, error) {
	return clickhouseConnector(fmt.Sprintf("%s?database=%s&username=%s&password=%s",
		cfg.Address,
		cfg.Database,
		cfg.Username,
		cfg.Password,
	))
}

func tlsConnector(cfg Configuration) (*sql.DB, error) {
	caCert, err := ioutil.ReadFile(cfg.CaFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	err = clickhouse.RegisterTLSConfig(tlsConfigKey, &tls.Config{RootCAs: caCertPool})
	if err != nil {
		return nil, err
	}
	return clickhouseConnector(fmt.Sprintf(
		"%s?username=%s&password=%s&secure=true&tls_config=%s&database=%s",
		cfg.Address,
		cfg.Username,
		cfg.Password,
		tlsConfigKey,
		cfg.Database,
	))
}

func clickhouseConnector(params string) (*sql.DB, error) {
	db, err := sql.Open("clickhouse", params)
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
			_ = tx.Rollback()
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
