package store

import (
	"context"
	"fmt"

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
func Open(dialect Dialect, dsn string) (*Store, error) {
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
