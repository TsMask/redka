package config

import (
	"context"
	"log/slog"
	"strings"

	"github.com/tsmask/redka/internal/store"

	"gorm.io/gorm"
)

// Tx represents a transaction context.
type Tx struct {
	db      *gorm.DB
	dialect store.Dialect
}

// Dialect returns the database dialect.
func (tx *Tx) Dialect() store.Dialect {
	return tx.dialect
}

// DB returns the GORM database instance.
func (tx *Tx) DB() *gorm.DB {
	return tx.db
}

// DB represents a database handle.
type DB struct {
	store *store.Store
}

// Dialect returns the database dialect.
func (db *DB) Dialect() store.Dialect {
	return db.store.Dialect
}

// Store returns the underlying store instance.
func (db *DB) Store() *store.Store {
	return db.store
}

// Transaction executes a transaction with the given function.
func (db *DB) Transaction(ctx context.Context, fn func(tx *Tx) error) error {
	return db.store.DB.WithContext(ctx).Transaction(func(gormTx *gorm.DB) error {
		tx := &Tx{
			db:      gormTx,
			dialect: db.store.Dialect,
		}
		return fn(tx)
	})
}

// Close closes the database handle.
func (db *DB) Close() error {
	return db.store.Close()
}

// OpenDB opens a new database handle from a DSN.
// Creates the database schema if necessary.
func OpenDB(dsn string, logger *slog.Logger) (*DB, error) {
	driverName, dsn := inferDriverNameAndDSN(dsn)
	sdb, err := store.Open(driverName, dsn)
	if err != nil {
		return nil, err
	}

	rdb := &DB{
		store: sdb,
	}
	return rdb, nil
}

// inferDriverNameAndDSN infers the database driver name and DSN from the DSN prefix.
func inferDriverNameAndDSN(dsn string) (store.Dialect, string) {
	// PostgreSQL: postgres:// or postgresql:// or pgx://
	if strings.HasPrefix(dsn, "postgres://") {
		return "postgres", dsn
	} else if strings.HasPrefix(dsn, "postgresql://") {
		return "postgres", dsn
	} else if strings.HasPrefix(dsn, "pgx://") {
		return "postgres", strings.Replace(dsn, "pgx://", "postgres://", 1)
	}

	// MySQL: mysql:// or mariadb:// or user@tcp(host)
	if strings.HasPrefix(dsn, "mysql://") {
		return "mysql", dsn[8:]
	} else if strings.HasPrefix(dsn, "mariadb://") {
		return "mysql", dsn[10:]
	} else if strings.Contains(dsn, "@tcp(") {
		return "mysql", dsn
	}

	// SQLite: file:, sqlite:, sqlite3:, or any other format (default)
	// SQLite DSN formats:
	// - file:/path/to/db.db
	// - sqlite:/path/to/db.db
	// - sqlite3:/path/to/db.db
	// Or just a plain file path like "/path/to/db.db"
	if strings.HasPrefix(dsn, "sqlite:") {
		return "sqlite", strings.Replace(dsn, "sqlite:", "file:", 1)
	} else if strings.HasPrefix(dsn, "sqlite3:") {
		return "sqlite", strings.Replace(dsn, "sqlite3:", "file:", 1)
	}
	return "sqlite", dsn
}
