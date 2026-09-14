package store

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// Store is a database handle using GORM.
type Store struct {
	Dialect Dialect  // database dialect
	DB      *gorm.DB // primary database handle

	writeCommitter *writeCommitter
	inTx           bool          // true when DB is already a transaction
	writeMu        *sync.RWMutex // gates destructive flush operations
	cleanupTicker  *time.Ticker  // timer for periodic cleanup
	cleanupStop    chan struct{} // channel to stop cleanup goroutine
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
	store.writeMu = &sync.RWMutex{}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	store.writeCommitter = newWriteCommitter(store)
	return store, nil
}

func (s *Store) Transaction(ctx context.Context, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	if s.inTx {
		return fn(s.DB.WithContext(ctx), s.Dialect)
	}
	s.writeMu.RLock()
	defer s.writeMu.RUnlock()
	return s.rawTransaction(ctx, fn)
}

// ExclusiveTransaction waits for already-started transactions and blocks new
// ones while fn runs. FLUSHDB and FLUSHALL use it to establish a stable
// barrier against concurrent command writers.
func (s *Store) ExclusiveTransaction(ctx context.Context, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	if s.inTx {
		return fn(s.DB.WithContext(ctx), s.Dialect)
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return s.rawTransaction(ctx, fn)
}

func (s *Store) rawTransaction(ctx context.Context, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	return s.DB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return fn(tx, s.Dialect)
	})
}

// WithContext returns a request-scoped copy while preserving shared services
// such as the write committer.
func (s *Store) WithContext(ctx context.Context) *Store {
	requestStore := *s
	requestStore.DB = s.DB.WithContext(ctx)
	return &requestStore
}

// InTx reports whether this handle is bound to an existing transaction.
func (s *Store) InTx() bool {
	return s.inTx
}

// FromTx wraps an existing database transaction. Repository calls to
// Transaction then reuse the transaction instead of creating a savepoint.
func FromTx(dialect Dialect, db *gorm.DB) *Store {
	return &Store{Dialect: dialect, DB: db, inTx: true}
}

// Close closes the underlying sql.DB connection and stops the cleanup service.
func (s *Store) Close() error {
	if s.writeCommitter != nil {
		s.writeCommitter.close()
	}

	// Stop the cleanup service
	if s.cleanupStop != nil {
		close(s.cleanupStop)
		s.cleanupStop = nil
		s.cleanupTicker = nil
	}

	// Close the database connection
	if s.DB == nil {
		return nil
	}
	if sqlDB, err := s.DB.DB(); err == nil {
		return sqlDB.Close()
	}
	return nil
}

// Version returns the database server version string.
func (s *Store) Version() (string, error) {
	var version string
	switch s.Dialect {
	case DialectPostgres, DialectMySQL:
		if err := s.DB.Raw("SELECT version()").Scan(&version).Error; err != nil {
			return "", err
		}
	case DialectSQLite:
		if err := s.DB.Raw("SELECT sqlite_version()").Scan(&version).Error; err != nil {
			return "", err
		}
	}
	return version, nil
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

// CleanupExpiredKeys cleans up expired keys from the database.
// This performs a one-time cleanup and returns the number of deleted keys.
// Note: This uses separate DELETE statements without transaction to avoid deadlocks
// in high-concurrency scenarios.
func (s *Store) CleanupExpiredKeys() (int, error) {
	now := s.DB.NowFunc().UnixMilli()
	var deleted int

	err := s.Transaction(context.Background(), func(tx *gorm.DB, _ Dialect) error {
		query := tx.Model(&RKey{}).
			Where("expire_at IS NOT NULL AND expire_at <= ?", now)
		if s.Dialect == DialectMySQL || s.Dialect == DialectPostgres {
			query = query.Clauses(clause.Locking{Strength: "UPDATE"})
		}

		var expiredIDs []int
		if err := query.Pluck("id", &expiredIDs).Error; err != nil {
			return err
		}
		if len(expiredIDs) == 0 {
			return nil
		}

		for _, table := range []string{"rstring", "rhash", "rlist", "rset", "rzset"} {
			if err := tx.Exec("DELETE FROM "+table+" WHERE kid IN ?", expiredIDs).Error; err != nil {
				return err
			}
		}
		result := tx.Where("id IN ?", expiredIDs).Delete(&RKey{})
		deleted = int(result.RowsAffected)
		return result.Error
	})

	return deleted, err
}

// StartCleanupTicker starts a background goroutine that periodically cleans up
// expired keys. The interval parameter controls how often cleanup runs.
// This method starts the cleanup goroutine and stores the ticker/stop channel
// in the Store struct. Call Close() to stop the cleanup service.
func (s *Store) StartCleanupTicker(interval time.Duration) {
	s.cleanupStop = make(chan struct{})
	s.cleanupTicker = time.NewTicker(interval)

	go func() {
		defer s.cleanupTicker.Stop()
		for {
			select {
			case <-s.cleanupTicker.C:
				deleted, err := s.CleanupExpiredKeys()
				if err != nil {
					slog.Warn("cleanup expired keys", "error", err)
				} else if deleted > 0 {
					slog.Info("cleanup expired keys", "deleted", deleted)
				}
			case <-s.cleanupStop:
				slog.Info("cleanup service stopped")
				return
			}
		}
	}()
}

// gormConfig returns the GORM configuration.
func gormConfig() *gorm.Config {
	return gormConfigWithPreparedStatements(true)
}

func gormConfigWithPreparedStatements(preparedStmt bool) *gorm.Config {
	return &gorm.Config{
		// Repository code already opens transactions for multi-statement
		// invariants. Skipping GORM's implicit transaction avoids a second
		// commit on one-statement writes.
		SkipDefaultTransaction:   true,
		DisableNestedTransaction: true,
		PrepareStmt:              preparedStmt,
		Logger:                   logger.Default.LogMode(logger.Silent),
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
