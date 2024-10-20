package postgres

import (
	"context"
	"log"
	"time"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/lib/pq"

	"github.com/doug-martin/goqu/v9"
	"github.com/jmoiron/sqlx"

	database "github.com/humangrass/gommon/database"
)

type Pool struct {
	db *goqu.Database
}

func (c *Pool) Builder() *goqu.Database {
	return c.db
}

// Drop close not implemented in database.
func (c *Pool) Drop() error {
	return nil
}

func (c *Pool) DropMsg() string {
	return "close database: is not implemented"
}

func NewPool(ctx context.Context, opt *database.Opt) (*Pool, error) {
	db, err := sqlx.Open(opt.Dialect, opt.ConnectionString())
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err = db.PingContext(pingCtx); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(opt.MaxIdleConns)
	db.SetMaxOpenConns(opt.MaxOpenConns)
	db.SetConnMaxLifetime(opt.MaxConnMaxLifetime)

	dialect := goqu.Dialect(opt.Dialect)
	pool := dialect.DB(db)

	if opt.Debug {
		logger := &database.Logger{}
		logger.SetCallback(func(format string, v ...interface{}) {
			log.Println(v)
		})
		pool.Logger(logger)
	}

	return &Pool{db: pool}, nil
}

// NewTestPoolFromDsn only for tests.
func NewTestPoolFromDsn(ctx context.Context, del, dsn string, debug bool) (*Pool, error) {
	db, err := sqlx.Open(del, dsn)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err = db.PingContext(pingCtx); err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(2)
	db.SetMaxOpenConns(2)
	db.SetConnMaxLifetime(time.Minute)

	dialect := goqu.Dialect(del)
	pool := dialect.DB(db)

	if debug {
		logger := &database.Logger{}
		logger.SetCallback(func(format string, v ...interface{}) {
			log.Println(v)
		})
		pool.Logger(logger)
	}

	return &Pool{db: pool}, nil
}
