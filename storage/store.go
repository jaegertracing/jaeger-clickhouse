package storage

import (
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"

	jaegerclickhouse "github.com/jaegertracing/jaeger-clickhouse"

	clickhouse "github.com/ClickHouse/clickhouse-go"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"

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

	if err := runInitScripts(logger, db, cfg); err != nil {
		_ = db.Close()
		return nil, err
	}
	if cfg.Replication {
		return &Store{
			db: db,
			writer: clickhousespanstore.NewSpanWriter(logger, db, cfg.SpansIndexTable, cfg.SpansTable,
				clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize, cfg.MaxSpanCount),
			reader: clickhousespanstore.NewTraceReader(db, cfg.OperationsTable, cfg.SpansIndexTable, cfg.SpansTable, cfg.MaxNumSpans),
			archiveWriter: clickhousespanstore.NewSpanWriter(logger, db, "", cfg.GetSpansArchiveTable(),
				clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize, cfg.MaxSpanCount),
			archiveReader: clickhousespanstore.NewTraceReader(db, "", "", cfg.GetSpansArchiveTable(), cfg.MaxNumSpans),
		}, nil
	}
	return &Store{
		db: db,
		writer: clickhousespanstore.NewSpanWriter(logger, db, cfg.SpansIndexTable, cfg.SpansTable,
			clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize, cfg.MaxSpanCount),
		reader: clickhousespanstore.NewTraceReader(db, cfg.OperationsTable, cfg.SpansIndexTable, cfg.SpansTable, cfg.MaxNumSpans),
		archiveWriter: clickhousespanstore.NewSpanWriter(logger, db, "", cfg.GetSpansArchiveTable(),
			clickhousespanstore.Encoding(cfg.Encoding), cfg.BatchFlushInterval, cfg.BatchWriteSize, cfg.MaxSpanCount),
		archiveReader: clickhousespanstore.NewTraceReader(db, "", "", cfg.GetSpansArchiveTable(), cfg.MaxNumSpans),
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

type tableArgs struct {
	Database string

	SpansIndexTable   clickhousespanstore.TableName
	SpansTable        clickhousespanstore.TableName
	OperationsTable   clickhousespanstore.TableName
	SpansArchiveTable clickhousespanstore.TableName

	TTLTimestamp string
	TTLDate      string

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
			sqlStatement, err := ioutil.ReadFile(filepath.Clean(f))
			if err != nil {
				return err
			}
			sqlStatements = append(sqlStatements, string(sqlStatement))
		}
	} else {
		templates := template.Must(template.ParseFS(jaegerclickhouse.SQLScripts, "sqlscripts/*.tmpl.sql"))

		args := tableArgs{
			Database: cfg.Database,

			SpansIndexTable:   cfg.SpansIndexTable,
			SpansTable:        cfg.SpansTable,
			OperationsTable:   cfg.OperationsTable,
			SpansArchiveTable: cfg.GetSpansArchiveTable(),

			TTLTimestamp: ttlTimestamp,
			TTLDate:      ttlDate,

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
