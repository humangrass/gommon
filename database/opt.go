package go_database

import (
	"fmt"
	"time"
)

const (
	defaultDialect   = "postgres"
	defaultLocalHost = "@"
)

type Opt struct {
	Host               string        `yaml:"host"`
	User               string        `yaml:"user"`
	Password           string        `yaml:"password"`
	Port               string        `yaml:"port"`
	Database           string        `yaml:"database"`
	Dialect            string        `yaml:"dialect"`
	Debug              bool          `yaml:"debug"`
	MaxIdleConns       int           `yaml:"max_idle_conns"`
	MaxOpenConns       int           `yaml:"max_open_conns"`
	MaxConnMaxLifetime time.Duration `yaml:"max_conn_max_lifetime"`
	SSL                string        `yaml:"ssl_mode"`
}

func (o *Opt) UnwrapOrPanic() {
	switch o.Dialect {
	case "":
		o.Dialect = defaultDialect
	case "postgres":
	default:
		panic("unsupported database dialect")
	}

	if o.Host == "" {
		o.Host = defaultLocalHost
	}

	if o.MaxIdleConns <= 0 {
		panic("max_idle_conns must be greater than zero")
	}
	if o.MaxOpenConns <= 0 {
		panic("max_open_conns must be greater than zero")
	}
	if o.MaxConnMaxLifetime <= 0 {
		panic("max_conn_max_lifetime must be greater than zero")
	}
}

func (o *Opt) ConnectionString() string {
	return fmt.Sprintf(
		"user=%s password=%s host=%s port=%s dbname=%s sslmode=%s",
		o.User,
		o.Password,
		o.Host,
		o.Port,
		o.Database,
		o.SSL,
	)
}
