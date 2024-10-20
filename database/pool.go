package go_database

import "github.com/doug-martin/goqu/v9"

// Pool is common database pool interface.
type Pool interface {
	Builder() *goqu.Database
}
