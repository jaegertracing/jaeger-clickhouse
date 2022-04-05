package clickhousespanstore

import (
	"database/sql"
	"time"

	hclog "github.com/hashicorp/go-hclog"
)

// WorkerParams contains parameters that are shared between WriteWorkers
type WorkerParams struct {
	logger     hclog.Logger
	db         *sql.DB
	indexTable TableName
	spansTable TableName
	tenant     string
	encoding   Encoding
	delay      time.Duration
}
