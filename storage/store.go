package storage

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"embed"
	"fmt"
	jaegerclickhouse "github.com/pavolloffay/jaeger-clickhouse"
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

const (
	tlsConfigKey = "clickhouse_tls_config_key"
)

var (
	_ shared.StoragePlugin        = (*Store)(nil)
	_ shared.ArchiveStoragePlugin = (*Store)(nil)
	_ io.Closer                   = (*Store)(nil)
)

func NewStore(logger hclog.Logger, cfg Configuration) (*Store, error) {
	cfg.setDefaults()
	db, err := connector(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := initializeDB(db, cfg); err != nil {
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

func initializeDB(db *sql.DB, cfg Configuration) error {
	var embeddedScripts embed.FS
	if cfg.Replication {
		embeddedScripts = jaegerclickhouse.EmbeddedFilesReplication
	} else {
		embeddedScripts = jaegerclickhouse.EmbeddedFilesNoReplication
	}

	var sqlStatements []string
	switch {
	case cfg.InitSQLScriptsDir != "":
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
			sqlStatements = append(sqlStatements, string(sqlStatement))
		}
	case cfg.Replication:
		f, err := embeddedScripts.ReadFile("sqlscripts/replication/0001-database.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0002-use-jaeger.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, string(f))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0003-jaeger-index-local.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.SpansIndexTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0004-jaeger-spans-local.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.SpansTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0005-jaeger-operations-local.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.OperationsTable), toLocal(cfg.SpansIndexTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0006-jaeger-spans-archive-local.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.GetSpansArchiveTable())))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0007-distributed-city-hash.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(
			string(f),
			cfg.SpansTable,
			addDbName(toLocal(cfg.SpansTable)),
			toLocal(cfg.SpansTable),
		))
		sqlStatements = append(sqlStatements, fmt.Sprintf(
			string(f),
			cfg.SpansIndexTable,
			addDbName(toLocal(cfg.SpansIndexTable)),
			toLocal(cfg.SpansIndexTable),
		))
		sqlStatements = append(sqlStatements, fmt.Sprintf(
			string(f),
			cfg.GetSpansArchiveTable(),
			addDbName(toLocal(cfg.GetSpansArchiveTable())),
			toLocal(cfg.GetSpansArchiveTable()),
		))
		f, err = embeddedScripts.ReadFile("sqlscripts/replication/0008-distributed-rand.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(
			string(f),
			cfg.OperationsTable,
			addDbName(toLocal(cfg.OperationsTable)),
			toLocal(cfg.OperationsTable),
		))
	default:
		f, err := embeddedScripts.ReadFile("sqlscripts/no-replication/0001-jaeger-index.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.SpansIndexTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/no-replication/0002-jaeger-spans.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.SpansTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/no-replication/0003-jaeger-operations.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.OperationsTable), toLocal(cfg.SpansIndexTable)))
		f, err = embeddedScripts.ReadFile("sqlscripts/no-replication/0004-jaeger-spans-archive.sql")
		if err != nil {
			return err
		}
		sqlStatements = append(sqlStatements, fmt.Sprintf(string(f), toLocal(cfg.GetSpansArchiveTable())))
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

	for _, statement := range sqlStatements {
		_, err = tx.Exec(statement)
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
