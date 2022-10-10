package storage

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"time"

	clickhouse "github.com/ClickHouse/clickhouse-go/v2"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

	jaegerclickhouse "github.com/jaegertracing/jaeger-clickhouse"
	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousedependencystore"
	"github.com/jaegertracing/jaeger-clickhouse/storage/clickhousespanstore"
)

type Store struct {
	db            *sql.DB
	writer        spanstore.Writer
	reader        spanstore.Reader
	archiveWriter spanstore.Writer
	archiveReader spanstore.Reader
}

var (
	_ shared.StoragePlugin             = (*Store)(nil)
	_ shared.ArchiveStoragePlugin      = (*Store)(nil)
	_ shared.StreamingSpanWriterPlugin = (*Store)(nil)
	_ io.Closer                        = (*Store)(nil)
)

func NewStore(logger hclog.Logger, cfg Configuration) (*Store, error) {
	cfg.setDefaults()
	db, err := connector(cfg)
	if err != nil {
		return nil, fmt.Errorf("could not connect to database: %q", err)
	}

	if err := runInitScripts(logger, db, cfg); err != nil {
		_ = db.Close()
		return nil, err
	}
	if cfg.Replication {
		return &Store{
			db: db,
			writer: clickhousespanstore.NewSpanWriter(
				logger,
				db,
				cfg.SpansIndexTable,
				cfg.SpansTable,
				cfg.Tenant,
				clickhousespanstore.Encoding(cfg.Encoding),
				cfg.BatchFlushInterval,
				cfg.BatchWriteSize,
				cfg.MaxSpanCount,
			),
			reader: clickhousespanstore.NewTraceReader(
				db,
				cfg.OperationsTable,
				cfg.SpansIndexTable,
				cfg.SpansTable,
				cfg.Tenant,
				cfg.MaxNumSpans,
			),
			archiveWriter: clickhousespanstore.NewSpanWriter(
				logger,
				db,
				"",
				cfg.GetSpansArchiveTable(),
				cfg.Tenant,
				clickhousespanstore.Encoding(cfg.Encoding),
				cfg.BatchFlushInterval,
				cfg.BatchWriteSize,
				cfg.MaxSpanCount,
			),
			archiveReader: clickhousespanstore.NewTraceReader(
				db,
				"",
				"",
				cfg.GetSpansArchiveTable(),
				cfg.Tenant,
				cfg.MaxNumSpans,
			),
		}, nil
	}
	return &Store{
		db: db,
		writer: clickhousespanstore.NewSpanWriter(
			logger,
			db,
			cfg.SpansIndexTable,
			cfg.SpansTable,
			cfg.Tenant,
			clickhousespanstore.Encoding(cfg.Encoding),
			cfg.BatchFlushInterval,
			cfg.BatchWriteSize,
			cfg.MaxSpanCount,
		),
		reader: clickhousespanstore.NewTraceReader(
			db,
			cfg.OperationsTable,
			cfg.SpansIndexTable,
			cfg.SpansTable,
			cfg.Tenant,
			cfg.MaxNumSpans,
		),
		archiveWriter: clickhousespanstore.NewSpanWriter(
			logger,
			db,
			"",
			cfg.GetSpansArchiveTable(),
			cfg.Tenant,
			clickhousespanstore.Encoding(cfg.Encoding),
			cfg.BatchFlushInterval,
			cfg.BatchWriteSize,
			cfg.MaxSpanCount,
		),
		archiveReader: clickhousespanstore.NewTraceReader(
			db,
			"",
			"",
			cfg.GetSpansArchiveTable(),
			cfg.Tenant,
			cfg.MaxNumSpans,
		),
	}, nil
}

func connector(cfg Configuration) (*sql.DB, error) {
	var conn *sql.DB

	options := clickhouse.Options{
		Addr: []string{sanitize(cfg.Address)},
		Auth: clickhouse.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		Compression: &clickhouse.Compression{
			Method: clickhouse.CompressionLZ4,
		},
	}

	if cfg.CaFile != "" {
		caCert, err := os.ReadFile(cfg.CaFile)
		if err != nil {
			return nil, err
		}
		caCertPool := x509.NewCertPool()
		caCertPool.AppendCertsFromPEM(caCert)
		options.TLS = &tls.Config{
			RootCAs: caCertPool,
		}
	}
	conn = clickhouse.OpenDB(&options)

	if cfg.MaxOpenConns != nil {
		conn.SetMaxIdleConns(int(*cfg.MaxOpenConns))
	}
	if cfg.MaxIdleConns != nil {
		conn.SetMaxIdleConns(int(*cfg.MaxIdleConns))
	}
	if cfg.ConnMaxLifetimeMillis != nil {
		conn.SetConnMaxLifetime(time.Millisecond * time.Duration(*cfg.ConnMaxLifetimeMillis))
	}
	if cfg.ConnMaxIdleTimeMillis != nil {
		conn.SetConnMaxIdleTime(time.Millisecond * time.Duration(*cfg.ConnMaxIdleTimeMillis))
	}

	if err := conn.Ping(); err != nil {
		return nil, err
	}
	return conn, nil
}

type tableArgs struct {
	Database string

	SpansIndexTable   clickhousespanstore.TableName
	SpansTable        clickhousespanstore.TableName
	OperationsTable   clickhousespanstore.TableName
	SpansArchiveTable clickhousespanstore.TableName

	TTLTimestamp string
	TTLDate      string

	Multitenant bool
	Replication bool
}

type distributedTableArgs struct {
	Database string
	Table    clickhousespanstore.TableName
	Hash     string
}

func render(templates *template.Template, filename string, args interface{}) string {
	var statement strings.Builder
	err := templates.ExecuteTemplate(&statement, filename, args)
	if err != nil {
		panic(err)
	}
	return statement.String()
}

func runInitScripts(logger hclog.Logger, db *sql.DB, cfg Configuration) error {
	var (
		sqlStatements []string
		ttlTimestamp  string
		ttlDate       string
	)
	if cfg.TTLDays > 0 {
		ttlTimestamp = fmt.Sprintf("TTL timestamp + INTERVAL %d DAY DELETE", cfg.TTLDays)
		ttlDate = fmt.Sprintf("TTL date + INTERVAL %d DAY DELETE", cfg.TTLDays)
	}
	if cfg.InitSQLScriptsDir != "" {
		filePaths, err := walkMatch(cfg.InitSQLScriptsDir, "*.sql")
		if err != nil {
			return fmt.Errorf("could not list sql files: %q", err)
		}
		sort.Strings(filePaths)
		for _, f := range filePaths {
			sqlStatement, err := os.ReadFile(filepath.Clean(f))
			if err != nil {
				return err
			}
			sqlStatements = append(sqlStatements, string(sqlStatement))
		}
	}
	if *cfg.InitTables {
		templates := template.Must(template.ParseFS(jaegerclickhouse.SQLScripts, "sqlscripts/*.tmpl.sql"))

		args := tableArgs{
			Database: cfg.Database,

			SpansIndexTable:   cfg.SpansIndexTable,
			SpansTable:        cfg.SpansTable,
			OperationsTable:   cfg.OperationsTable,
			SpansArchiveTable: cfg.GetSpansArchiveTable(),

			TTLTimestamp: ttlTimestamp,
			TTLDate:      ttlDate,

			Multitenant: cfg.Tenant != "",
			Replication: cfg.Replication,
		}

		if cfg.Replication {
			// Add "_local" to the local table names, and omit it from the distributed tables below
			args.SpansIndexTable = args.SpansIndexTable.ToLocal()
			args.SpansTable = args.SpansTable.ToLocal()
			args.OperationsTable = args.OperationsTable.ToLocal()
			args.SpansArchiveTable = args.SpansArchiveTable.ToLocal()
		}

		sqlStatements = append(sqlStatements, render(templates, "jaeger-index.tmpl.sql", args))
		sqlStatements = append(sqlStatements, render(templates, "jaeger-operations.tmpl.sql", args))
		sqlStatements = append(sqlStatements, render(templates, "jaeger-spans.tmpl.sql", args))
		sqlStatements = append(sqlStatements, render(templates, "jaeger-spans-archive.tmpl.sql", args))

		if cfg.Replication {
			// Now these tables omit the "_local" suffix
			distargs := distributedTableArgs{
				Table:    cfg.SpansTable,
				Database: cfg.Database,
				Hash:     "cityHash64(traceID)",
			}
			sqlStatements = append(sqlStatements, render(templates, "distributed-table.tmpl.sql", distargs))

			distargs.Table = cfg.SpansIndexTable
			sqlStatements = append(sqlStatements, render(templates, "distributed-table.tmpl.sql", distargs))

			distargs.Table = cfg.GetSpansArchiveTable()
			sqlStatements = append(sqlStatements, render(templates, "distributed-table.tmpl.sql", distargs))

			distargs.Table = cfg.OperationsTable
			distargs.Hash = "rand()"
			sqlStatements = append(sqlStatements, render(templates, "distributed-table.tmpl.sql", distargs))
		}
	}
	return executeScripts(logger, sqlStatements, db)
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

func (s *Store) StreamingSpanWriter() spanstore.Writer {
	return s.writer
}

func (s *Store) Close() error {
	return s.db.Close()
}

func executeScripts(logger hclog.Logger, sqlStatements []string, db *sql.DB) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	for _, statement := range sqlStatements {
		logger.Debug("Running SQL statement", "statement", statement)
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

// Earlier version of clickhouse-go used to expect address as tcp://host:port
// while newer version of clickhouse-go expect address as host:port (without scheme)
// so to maintain backward compatibility we clean it up
func sanitize(addr string) string {
	return strings.TrimPrefix(addr, "tcp://")
}
