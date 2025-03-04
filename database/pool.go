package database

import (
	"context"
	"database/sql"
	"github.com/doug-martin/goqu/v9"
)

// Pool is common database pool interface.
type Pool interface {
	Builder() *goqu.Database
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*goqu.TxDatabase, error)
}
