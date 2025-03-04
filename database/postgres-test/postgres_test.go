package postgres_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	_ "github.com/doug-martin/goqu/v9/dialect/postgres"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/humangrass/gommon/database"
	"github.com/humangrass/gommon/database/postgres"
)

var opt = &database.Opt{
	Host:               "localhost",
	User:               "user",
	Password:           "changeme123",
	Port:               "5432",
	Database:           "test",
	Dialect:            "postgres",
	Debug:              true,
	MaxIdleConns:       15,
	MaxOpenConns:       20,
	MaxConnMaxLifetime: 10 * time.Minute,
	SSL:                "disable",
}

func TestPool_BeginTx(t *testing.T) {
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, opt)
	require.NoError(t, err, "failed to create database pool")

	tx, err := pool.BeginTx(ctx, &sql.TxOptions{})
	require.NoError(t, err, "failed to begin transaction")

	_, err = tx.Exec("INSERT INTO test_table (name) VALUES ($1)", "test_name")
	require.NoError(t, err, "failed to execute query within transaction")

	err = tx.Commit()
	require.NoError(t, err, "failed to commit transaction")

	var count int
	err = pool.Builder().QueryRow("SELECT COUNT(*) FROM test_table WHERE name = $1", "test_name").Scan(&count)
	require.NoError(t, err, "failed to verify inserted data")
	require.Equal(t, 1, count, "data verification failed")

	// Cleanup
	_, err = pool.Builder().Exec("DELETE FROM test_table WHERE name = $1", "test_name")
	require.NoError(t, err, "failed to clean up test data")
}

func TestPool_Drop(t *testing.T) {
	ctx := context.Background()
	pool, err := postgres.NewPool(ctx, opt)
	require.NoError(t, err, "failed to create database pool")

	err = pool.Drop()
	require.NoError(t, err, "failed to drop database connection")

	err = pool.Drop()
	require.NoError(t, err, "repeated drop should not cause an error")
}
