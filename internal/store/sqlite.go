package store

import (
	"net/url"
	"strings"
	"time"

	"github.com/libtnb/sqlite"
	"gorm.io/gorm"
)

// newSQLite creates a new SQLite database handle using GORM.
func newSQLite(dsn string) (*Store, error) {
	source, _, _ := strings.Cut(dsn, "?")
	memory := source == ":memory:" || source == "file::memory:"
	dsn = sqliteDataSource(dsn)

	// Open the database connection
	db, err := gorm.Open(sqlite.Open(dsn), gormConfig())
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect: DialectSQLite,
		DB:      db,
	}

	// Configure connection pool
	if err := store.configurePoolSQLite(memory); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolSQLite sets the number of connections for SQLite.
// With WAL mode enabled, SQLite supports concurrent readers.
// We use 1 writer (enforced by SQLite locking) + multiple readers.
// MaxOpenConns=1 would serialize everything; a higher value allows
// concurrent reads which is critical for read-heavy workloads.
func (s *Store) configurePoolSQLite(memory bool) error {
	sqlDB, err := s.DB.DB()
	if err != nil {
		return err
	}
	maxOpenConns := 8
	maxIdleConns := 4
	if memory {
		// Each private in-memory SQLite connection has its own database.
		// Restricting the pool to one connection preserves schema and data
		// correctness; file-backed databases retain WAL concurrency.
		maxOpenConns = 1
		maxIdleConns = 1
	}
	sqlDB.SetMaxOpenConns(maxOpenConns)
	sqlDB.SetMaxIdleConns(maxIdleConns)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	return nil
}

// sqliteDataSource returns an SQLite connection string.
func sqliteDataSource(path string) string {
	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	// Redka manages one process-level connection pool. Private caches let WAL
	// handle cross-connection concurrency; shared-cache mode would reintroduce
	// SQLite table locks and conflict with concurrent command execution.
	params.Del("cache")

	// Keep explicit driver parameters and pragmas. A user-provided pragma of
	// the same name intentionally wins over the Redka default.
	existingPragmas := make(map[string]bool, len(params["_pragma"]))
	for _, pragma := range params["_pragma"] {
		name, _, _ := strings.Cut(pragma, "(")
		name, _, _ = strings.Cut(name, "=")
		if name != "" {
			existingPragmas[name] = true
		}
	}
	defaultPragmas := []string{
		"journal_mode(WAL)",
		"synchronous(NORMAL)",
		"wal_autocheckpoint(5000)",
		"busy_timeout(5000)",
		"mmap_size(268435456)",
		"locking_mode(NORMAL)",
		"journal_size_limit(16777216)",
		"cache_size(-64000)",
		"temp_store(MEMORY)",
		"page_size=4096",
		"foreign_keys=ON",
	}
	for _, pragma := range defaultPragmas {
		name, _, _ := strings.Cut(pragma, "(")
		if !existingPragmas[name] {
			params.Add("_pragma", pragma)
		}
	}

	if source == ":memory:" {
		// This is an in-memory database, so we must either enable shared cache
		// (https://sqlite.org/sharedcache.html), which is discouraged,
		// or use the memdb VFS (https://sqlite.org/src/file?name=src/memdb.c).
		// https://github.com/ncruces/go-sqlite3/issues/94#issuecomment-2157679766
		source = "file:/tmp/redka.sqlite"
		params.Set("vfs", "memdb")
	} else {
		// This is a file-based database, it must have a "file:" prefix
		// for setting parameters (https://www.sqlite.org/c3ref/open.html).
		if !strings.HasPrefix(source, "file:") {
			source = "file:" + source
		}
	}

	// sql.DB is concurrent-safe, so we don't need SQLite mutexes.
	params.Set("_mutex", "no")

	// Enable IMMEDIATE transactions for writable databases.
	// https://www.sqlite.org/lang_transaction.html
	params.Set("_txlock", "immediate")
	return source + "?" + params.Encode()
}
