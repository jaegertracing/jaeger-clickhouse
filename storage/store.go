package storage

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"embed"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"

	"github.com/ClickHouse/clickhouse-go"
	"github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/pavolloffay/jaeger-clickhouse/storage/clickhousespanstore"
)

type Store struct {
	db            *sql.DB
	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
}

type customStatement struct {
	query  string
	params []string
}

func newCustomStatement(query string, params ...string) customStatement {
	return customStatement{query: query, params: params}
}

func (statement *customStatement) exec(tx *sql.Tx) (sql.Result, error) {
	return tx.Exec(statement.query, statement.params)
}

const (
	tlsConfigKey = "clickhouse_tls_config_key"
)

var _ shared.StoragePlugin = (*Store)(nil)
var _ shared.ArchiveStoragePlugin = (*Store)(nil)
var _ io.Closer = (*Store)(nil)

func NewStore(logger hclog.Logger, cfg Configuration, embeddedSQLScripts embed.FS) (*Store, error) {
	cfg.setDefaults()
	db, err := connector(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := initializeDB(db, cfg, embeddedSQLScripts); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{
		db:            db,
		writer:        clickhousespanstore.NewSpanWriter(logger, db, cfg.SpansIndexTable, cfg.SpansTable, clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		reader:        clickhousespanstore.NewTraceReader(db, cfg.OperationsTable, cfg.SpansIndexTable, cfg.SpansTable),
		archiveWriter: clickhousespanstore.NewSpanWriter(logger, db, "", "jaeger_archive_spans", clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize),
		archiveReader: clickhousespanstore.NewTraceReader(db, "", "", "jaeger_archive_spans"),
	}, nil
}

func connector(cfg Configuration) (*sql.DB, error) {
	params := fmt.Sprintf("%s?database=%s&username=%s&password=%s",
		cfg.Address,
		cfg.Database,
		cfg.Username,
		cfg.Password,
	)

	if cfg.CaFile != "" {
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
		params += fmt.Sprintf(
			"&secure=true&tls_config=%s",
			tlsConfigKey,
		)
	}
	return clickhouseConnector(params)
}

func initializeDB(db *sql.DB, cfg Configuration, embeddedScripts embed.FS) error {
	var sqlStatements []customStatement
	if cfg.InitSQLScriptsDir != "" {
		filePaths, err := walkMatch(cfg.InitSQLScriptsDir, "*.sql")
		if err != nil {
			return fmt.Errorf("could not list sql files: %q", err)
		}
		sort.Strings(filePaths)
		for _, f := range filePaths {
			sqlStatement, err := ioutil.ReadFile(filepath.Clean(f))
			if err != nil {
				return err
			}
			sqlStatements = append(sqlStatements, newCustomStatement(string(sqlStatement)))
		}
	} else {
		f, err := embeddedScripts.ReadFile("sqlscripts/0001-jaeger-index.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, newCustomStatement(string(f), cfg.SpansIndexTable))
		f, err = embeddedScripts.ReadFile("sqlscripts/0002-jaeger-spans.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, newCustomStatement(string(f), cfg.SpansTable))
		f, err = embeddedScripts.ReadFile("sqlscripts/0003-jaeger-operations.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, newCustomStatement(string(f), cfg.OperationsTable))
		f, err = embeddedScripts.ReadFile("sqlscripts/0004-jaeger-spans-archive.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, newCustomStatement(string(f), cfg.getSpansArchiveTable()))
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

func executeScripts(sqlStatements []customStatement, db *sql.DB) error {
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

	for _, statement := range sqlStatements {
		_, err = statement.exec(tx)
		if err != nil {
			return fmt.Errorf("could not run sql %q: %q", statement, err)
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
