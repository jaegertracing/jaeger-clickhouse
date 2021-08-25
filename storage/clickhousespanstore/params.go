package clickhousespanstore

import (
	"database/sql"
	"github.com/hashicorp/go-hclog"
	"time"
)

type WriteParams struct {
	logger     hclog.Logger
	db         *sql.DB
	indexTable TableName
	spansTable TableName
	encoding   Encoding
	delay      time.Duration
}
