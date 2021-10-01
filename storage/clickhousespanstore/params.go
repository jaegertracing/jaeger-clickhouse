package clickhousespanstore

import (
	"database/sql"
	"time"

	"github.com/hashicorp/go-hclog"
)

// WriteParams contains parameters that are shared between WriteWorker`s
type WriteParams struct {
	logger     hclog.Logger
	db         *sql.DB
	indexTable TableName
	spansTable TableName
	encoding   Encoding
	delay      time.Duration
}
