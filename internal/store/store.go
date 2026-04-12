package store

import (
	"context"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Store is a database handle using GORM.
type Store struct {
	Dialect Dialect  // database dialect
	DB      *gorm.DB // primary database handle
}

// Open creates a new database handle from a DSN.
// Creates the database schema if necessary.
func Open(dsn string) (*Store, error) {
	dialect, dsn := inferDriverNameAndDSN(dsn)
	var store *Store
	var err error
	switch dialect {
	case DialectSQLite:
		store, err = newSQLite(dsn)
	case DialectPostgres:
		store, err = newPostgres(dsn)
	case DialectMySQL:
		store, err = newMySQL(dsn)
	default:
		return nil, fmt.Errorf("unknown SQL dialect: %s", dialect)
	}
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) Transaction(ctx context.Context, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	return s.DB.WithContext(ctx).Transaction(func(gormTx *gorm.DB) error {
		return fn(gormTx, s.Dialect)
	})
}

// Close closes the underlying sql.DB connection.
func (s *Store) Close() error {
	if s.DB == nil {
		return nil
	}
	if sqlDB, err := s.DB.DB(); err == nil {
		return sqlDB.Close()
	}
	return nil
}

// Migrate runs database migrations using GORM AutoMigrate.
// Creates/updates tables from GORM model definitions.
// Version tracking, mtime updates, and cardinality (len) tracking
// are handled in Go application code, not via database triggers.
func (s *Store) Migrate() error {
	err := s.DB.AutoMigrate(
		&RKey{},
		&RString{},
		&RHash{},
		&RList{},
		&RSet{},
		&RZSet{},
	)

	if err != nil {
		return fmt.Errorf("auto migrate: %w", err)
	}
	return nil
}

// gormConfig returns the GORM configuration.
func gormConfig() *gorm.Config {
	return &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	}
}

// inferDriverNameAndDSN infers the database driver name and DSN from the DSN prefix.
func inferDriverNameAndDSN(dsn string) (Dialect, string) {
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
