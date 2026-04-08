package store

import (
	"net/url"
	"strings"

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
func newSQLite(dsn string, opts *Options) (*Store, error) {
	// Build RW and RO data sources
	rwDSN := sqliteDataSource(dsn, false, opts.Pragma)
	roDSN := sqliteDataSource(dsn, true, opts.Pragma)

	// Open RW connection
	rwDB, err := gorm.Open(sqlite.Open(rwDSN), gormConfig())
	if err != nil {
		return nil, err
	}

	// Open RO connection
	roDB, err := gorm.Open(sqlite.Open(roDSN), gormConfig())
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect:        DialectSQLite,
		RW:             rwDB,
		RO:             roDB,
		Timeout:        opts.Timeout,
		MaxPoolConns:   opts.MaxPoolConns,
		MinPoolConns:   opts.MinPoolConns,
	}

	// Configure connection pools
	if err := store.configurePoolsSQLite(opts.ReadOnly); err != nil {
		return nil, err
	}

	// Apply pragmas
	if err := store.applySQLitePragmas(opts.Pragma); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolsSQLite sets the number of connections for SQLite.
func (s *Store) configurePoolsSQLite(readOnly bool) error {
	// For the read-only DB handle the number of open connections
	// should be equal to the number of idle connections. Otherwise,
	// the handle will keep opening and closing connections, severely
	// impacting the throughput.
	maxConns := s.MaxPoolConns
	if maxConns == 0 {
		maxConns = suggestNumConns()
	}
	minIdle := s.MinPoolConns
	if minIdle == 0 {
		minIdle = 2
	}

	roSqlDB, err := s.RO.DB()
	if err != nil {
		return err
	}
	configurePool(roSqlDB, maxConns, minIdle)

	if !readOnly {
		// SQLite allows only one writer at a time. Setting the maximum
		// number of DB connections to 1 for the read-write DB handle
		// is the best and fastest way to enforce this.
		rwSqlDB, err := s.RW.DB()
		if err != nil {
			return err
		}
		configurePool(rwSqlDB, 1, 1)
	}

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
	// use the connection hook (see cmd/redka/main.go on how to do this).
	// But since we can't be sure the user does that, we also set them here.
	//
	// Unfortunately, setting pragmas using Exec only sets them for
	// a single connection. It's not a problem for s.RW (which has only
	// one connection), but it is for s.RO (which has multiple connections).
	// Still, it's better than nothing.
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

	queryStr := query.String()
	if err := s.RW.Exec(queryStr).Error; err != nil {
		return err
	}
	if err := s.RO.Exec(queryStr).Error; err != nil {
		return err
	}
	return nil
}

// sqliteDataSource returns an SQLite connection string
// for a read-only or read-write mode.
func sqliteDataSource(path string, readOnly bool, pragma map[string]string) string {
	var ds string

	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	if source == ":memory:" {
		// This is an in-memory database, so we must either enable shared cache
		// (https://sqlite.org/sharedcache.html), which is discouraged,
		// or use the memdb VFS (https://sqlite.org/src/file?name=src/memdb.c).
		// https://github.com/ncruces/go-sqlite3/issues/94#issuecomment-2157679766
		ds = "file:/redka.db"
		params.Set("vfs", "memdb")
	} else {
		// This is a file-based database, it must have a "file:" prefix
		// for setting parameters (https://www.sqlite.org/c3ref/open.html).
		ds = source
		if !strings.HasPrefix(ds, "file:") {
			ds = "file:" + ds
		}
	}

	// sql.DB is concurrent-safe, so we don't need SQLite mutexes.
	params.Set("_mutex", "no")

	// Set the connection mode (writable or read-only).
	if readOnly {
		if params.Get("mode") != "memory" {
			// Enable read-only mode for read-only databases
			// (except for in-memory databases, which are always writable).
			// https://www.sqlite.org/c3ref/open.html
			params.Set("mode", "ro")
		}
	} else {
		// Enable IMMEDIATE transactions for writable databases.
		// https://www.sqlite.org/lang_transaction.html
		params.Set("_txlock", "immediate")
	}

	// Apply the pragma settings.
	// Some drivers (modernc and ncruces) setting passing pragmas
	// in the connection string, so we add them here.
	// The mattn driver does not support this, so it'll just ignore them.
	// For mattn driver, we have to set the pragmas in the connection hook.
	// (see cmd/redka/main.go on how to do this).
	if pragma == nil {
		pragma = sqlitePragma
	}
	for name, val := range pragma {
		params.Add("_pragma", name+"="+val)
	}

	return ds + "?" + params.Encode()
}
