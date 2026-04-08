package store

import (
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
	// If the map is empty, no options are set.
	Pragma map[string]string
	// Timeout for database operations.
	Timeout time.Duration
	// Whether the database is read-only.
	ReadOnly bool
	// MaxPoolConns is the maximum number of open connections in the pool.
	// If 0, uses a sensible default based on GOMAXPROCS.
	MaxPoolConns int
	// MinPoolConns is the minimum number of idle connections in the pool.
	// If 0, uses a conservative default (2).
	MinPoolConns int
}

// Store is a database handle using GORM.
// Has separate connection pools for read-write and read-only operations.
type Store struct {
	Dialect     Dialect       // database dialect
	RW          *gorm.DB      // read-write handle
	RO          *gorm.DB      // read-only handle
	Timeout     time.Duration // transaction timeout
	MaxPoolConns int          // max connections (0 = auto)
	MinPoolConns int          // min idle connections (0 = auto)
}

// Open creates a new database handle from a DSN.
// Creates the database schema if necessary.
func Open(dsn string, dialect Dialect, timeout time.Duration) (*Store, error) {
	opts := &Options{
		Dialect: dialect,
		Timeout: timeout,
	}
	return OpenWithOptions(dsn, opts)
}

// OpenWithOptions creates a new database handle from a DSN with custom options.
// Creates the database schema if necessary.
func OpenWithOptions(dsn string, opts *Options) (*Store, error) {
	store, err := NewWithOptions(dsn, opts)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	return store, nil
}

// New creates a new database handle from a DSN.
// Like Open, but does not create the database schema.
func New(dsn string, dialect Dialect, timeout time.Duration) (*Store, error) {
	opts := &Options{
		Dialect: dialect,
		Timeout: timeout,
	}
	return NewWithOptions(dsn, opts)
}

// NewWithOptions creates a new database handle from a DSN with custom options.
// Like OpenWithOptions, but does not create the database schema.
func NewWithOptions(dsn string, opts *Options) (*Store, error) {
	if opts.Timeout == 0 {
		opts.Timeout = DefaultTimeout
	}

	switch opts.Dialect {
	case DialectSQLite:
		return newSQLite(dsn, opts)
	case DialectPostgres:
		return newPostgres(dsn, opts)
	case DialectMySQL:
		return newMySQL(dsn, opts)
	default:
		return nil, ErrDialect
	}
}

// OpenDB creates a new Store using existing GORM instances.
// Creates the database schema if necessary.
func OpenDB(rw *gorm.DB, ro *gorm.DB, dialect Dialect, timeout time.Duration) (*Store, error) {
	store, err := NewDB(rw, ro, dialect, timeout)
	if err != nil {
		return nil, err
	}
	if err := store.Migrate(); err != nil {
		return nil, err
	}
	return store, nil
}

// NewDB creates a new Store using existing GORM instances.
// Like OpenDB, but does not create the database schema.
func NewDB(rw *gorm.DB, ro *gorm.DB, dialect Dialect, timeout time.Duration) (*Store, error) {
	if timeout == 0 {
		timeout = DefaultTimeout
	}
	return &Store{
		Dialect: dialect,
		RW:      rw,
		RO:      ro,
		Timeout: timeout,
	}, nil
}

// Close closes the underlying sql.DB connections.
func (s *Store) Close() error {
	var errs []error

	if s.RW != nil {
		if sqlDB, err := s.RW.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if s.RO != nil && s.RO != s.RW {
		if sqlDB, err := s.RO.DB(); err == nil {
			if err := sqlDB.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}

	if len(errs) > 0 {
		return errs[0]
	}
	return nil
}

// SQL returns the underlying *sql.DB for the read-write connection.
func (s *Store) SQL() (*sql.DB, error) {
	return s.RW.DB()
}

// SQLRO returns the underlying *sql.DB for the read-only connection.
func (s *Store) SQLRO() (*sql.DB, error) {
	return s.RO.DB()
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

// configurePool sets the connection pool size for a sql.DB.
func configurePool(sqlDB *sql.DB, maxOpen, maxIdle int) {
	sqlDB.SetMaxOpenConns(maxOpen)
	sqlDB.SetMaxIdleConns(maxIdle)
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Prevent stale connections
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)  // Reclaim idle connections
}
