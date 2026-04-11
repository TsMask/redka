package store

import (
	"net/url"
	"strings"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// sqlitePragma is a set of default SQLite settings.
var sqlitePragma = map[string]string{
	"journal_mode": "wal",
	"synchronous":  "normal",
	"temp_store":   "memory",
	"mmap_size":    "268435456",
	"foreign_keys": "on",
}

// newSQLite creates a new SQLite database handle using GORM.
func newSQLite(dsn string) (*Store, error) {
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
	if err := store.configurePoolSQLite(); err != nil {
		return nil, err
	}

	// Apply pragmas
	if err := store.applySQLitePragmas(sqlitePragma); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolSQLite sets the number of connections for SQLite.
func (s *Store) configurePoolSQLite() error {
	sqlDB, err := s.DB.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(100)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Prevent stale connections
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)  // Reclaim idle connections
	return nil
}

// applySQLitePragmas applies the database settings via PRAGMA statements.
func (s *Store) applySQLitePragmas(pragma map[string]string) error {
	// Ideally, we'd only set the pragmas in the connection string
	// (see sqliteDataSource), so we wouldn't need this function.
	// But since the mattn driver does not support setting pragmas
	// in the connection string, we also set them here.
	//
	// The correct way to set pragmas for the mattn driver is to
	// But since we can't be sure the user does that, we also set them here.
	//
	// Setting pragmas using Exec only sets them for a single connection.
	if pragma == nil {
		// If no pragmas are specified, use the default ones.
		pragma = sqlitePragma
	}

	if len(pragma) == 0 {
		// If there are no pragmas on purpose (empty map), don't do anything.
		return nil
	}

	var query strings.Builder
	for name, val := range pragma {
		query.WriteString("pragma ")
		query.WriteString(name)
		query.WriteString("=")
		query.WriteString(val)
		query.WriteString(";")
	}

	return s.DB.Exec(query.String()).Error
}

// sqliteDataSource returns an SQLite connection string.
func sqliteDataSource(path string) string {
	// Parse the parameters.
	source, query, hasQuery := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)
	if !hasQuery {
		// Apply the pragma settings.
		for name, val := range sqlitePragma {
			params.Add(name, val)
		}

	}

	if source == ":memory:" {
		// This is an in-memory database, so we must either enable shared cache
		// (https://sqlite.org/sharedcache.html), which is discouraged,
		// or use the memdb VFS (https://sqlite.org/src/file?name=src/memdb.c).
		// https://github.com/ncruces/go-sqlite3/issues/94#issuecomment-2157679766
		source = "file:/redka.db"
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
