package store

import (
	"context"
	"database/sql"
	"runtime"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// DefaultTimeout is the default timeout for database operations.
const DefaultTimeout = 5 * time.Second

// Options is the configuration for the database.
type Options struct {
	// SQL dialect.
	Dialect Dialect
	// Options to set on the database connection.
	// If nil, uses the engine-specific defaults.
	Pragma map[string]string
	// Timeout for database operations.
	Timeout time.Duration
	// MaxPoolConns is the maximum number of open connections in the pool.
	// If 0, uses a sensible default based on GOMAXPROCS.
	MaxPoolConns int
	// MinPoolConns is the minimum number of idle connections in the pool.
	// If 0, uses a conservative default (2).
	MinPoolConns int
}

// Store is a database handle using GORM.
type Store struct {
	Dialect      Dialect       // database dialect
	DB           *gorm.DB      // primary database handle
	Timeout      time.Duration // transaction timeout
	MaxPoolConns int           // max connections (0 = auto)
	MinPoolConns int           // min idle connections (0 = auto)
	dbIdx        int           // current logical database index (0-15)
}

// DBIdx returns the current logical database index.
func (s *Store) DBIdx() int {
	return s.dbIdx
}

// SetDBIdx sets the current logical database index.
// Exported for use by type packages via store.Store.
func (s *Store) SetDBIdx(idx int) {
	s.dbIdx = idx
}

// Open creates a new database handle from a DSN.
// Creates the database schema if necessary.
func Open(dsn string, dialect Dialect, timeout time.Duration) (*Store, error) {
	opts := &Options{
		Dialect: dialect,
		Timeout: timeout,
	}
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}

	var store *Store
	var err error
	switch dialect {
	case DialectSQLite:
		store, err = newSQLite(dsn, opts)
	case DialectPostgres:
		store, err = newPostgres(dsn, opts)
	case DialectMySQL:
		store, err = newMySQL(dsn, opts)
	default:
		return nil, ErrDialect
	}
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	return store, nil
}

// OpenDB creates a new Store using an existing GORM instance.
// Creates the database schema if necessary.
func OpenDB(db *gorm.DB, dialect Dialect, timeout time.Duration) (*Store, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	store := &Store{
		Dialect: dialect,
		DB:      db,
		Timeout: timeout,
	}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	return store, nil
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

// SQL returns the underlying *sql.DB.
func (s *Store) SQL() (*sql.DB, error) {
	return s.DB.DB()
}

// Ping verifies that the database connection is alive.
func (s *Store) Ping(ctx context.Context) error {
	if sqlDB, err := s.DB.DB(); err != nil {
		return err
	} else {
		return sqlDB.PingContext(ctx)
	}
}

// suggestNumConns calculates the optimal number
// of parallel connections to the database.
// Uses GOMAXPROCS to better utilize multi-core systems.
func suggestNumConns() int {
	ncpu := runtime.NumCPU()
	// Scale connections with cores: 2-16 based on CPU count
	// 8 is too conservative for modern multi-core servers
	switch {
	case ncpu < 2:
		return 2
	case ncpu < 4:
		return 4
	case ncpu < 8:
		return 8
	case ncpu < 16:
		return 16
	default:
		return 32 // GOMAXPROCS often higher; cap at reasonable max
	}
}

// gormConfig returns the GORM configuration.
func gormConfig() *gorm.Config {
	return &gorm.Config{
		PrepareStmt: true,
		Logger:      logger.Default.LogMode(logger.Silent),
	}
}

// Note on PrepareStmt:
// With PrepareStmt:true, GORM caches prepared statements at the sql.DB level.
// There is no built-in upper limit on cached statements. In environments with
// very high query diversity (many different key names, field names, etc.),
// the cache may grow indefinitely. To monitor this, poll sql.DB.Stats() and
// track open connection count. Consider setting MaxOpenConns to bound memory
// usage if you observe unbounded growth in production with diverse workloads.

// configurePool sets the connection pool size for a sql.DB.
func configurePool(sqlDB *sql.DB, maxOpen, maxIdle int) {
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Prevent stale connections
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)  // Reclaim idle connections
}

// ctxKeyDBIdx is the context key for the current database index.
var ctxKeyDBIdx = "dbIdx"

// CtxWithDBIdx returns a context that carries the given database index.
func CtxWithDBIdx(parent context.Context, dbIdx int) context.Context {
	return context.WithValue(parent, ctxKeyDBIdx, dbIdx)
}

// CtxDBIdx retrieves the database index from a context.
// Returns 0 if not set.
func CtxDBIdx(ctx context.Context) int {
	if v := ctx.Value(ctxKeyDBIdx); v != nil {
		if idx, ok := v.(int); ok {
			return idx
		}
	}
	return 0
}
