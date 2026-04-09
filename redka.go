package redka

import (
	"context"
	"database/sql"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/rhash"
	"github.com/tsmask/redka/internal/rkey"
	"github.com/tsmask/redka/internal/rlist"
	"github.com/tsmask/redka/internal/rset"
	"github.com/tsmask/redka/internal/rstring"
	"github.com/tsmask/redka/internal/rzset"
	"github.com/tsmask/redka/internal/store"

	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

// A TypeID identifies the type of the key and thus
// the data structure of the value with that key.
type TypeID = core.TypeID

// Key types.
const (
	TypeAny    = core.TypeAny
	TypeString = core.TypeString
	TypeList   = core.TypeList
	TypeSet    = core.TypeSet
	TypeHash   = core.TypeHash
	TypeZSet   = core.TypeZSet
)

// Common errors returned by data structure methods.
var (
	ErrKeyType   = core.ErrKeyType   // key type mismatch
	ErrNotFound  = core.ErrNotFound  // key or element not found
	ErrValueType = core.ErrValueType // invalid value type
)

// Key represents a key data structure.
// Each key uniquely identifies a data structure stored in the
// database (e.g. a string, a list, or a hash). There can be only one
// data structure with a given key, regardless of type. For example,
// you can't have a string and a hash map with the same key.
type Key = core.Key

// Value represents a value stored in a database (a byte slice).
// It can be converted to other scalar types.
type Value = core.Value

// Options is the configuration for the database.
type Options struct {
	// SQL driver name.
	// If empty, uses "sqlite3".
	DriverName string
	// Options to set on the database connection.
	// If nil, uses the engine-specific defaults.
	Pragma map[string]string
	// Timeout for database operations.
	// If zero, uses the default timeout of 5 seconds.
	Timeout time.Duration
	// Logger for the database. If nil, uses a silent logger.
	Logger *slog.Logger
}

// Application options defaults.
var defaultOptions = Options{
	DriverName: "sqlite3",
	Timeout:    5 * time.Second,
	Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
}

// DB is a Redis-like repository backed by a relational database.
// Provides access to data structures like keys, strings, and hashes.
//
// DB is safe for concurrent use by multiple goroutines as long as you use
// a single instance of DB throughout your program.
type DB struct {
	dbIdx    atomic.Int32  // logical database index; thread-safe via atomic operations
	store    *store.Store
	act      *store.Transactor[*Tx]
	hashDB   *rhash.DB
	keyDB    *rkey.DB
	listDB   *rlist.DB
	setDB    *rset.DB
	stringDB *rstring.DB
	zsetDB   *rzset.DB
	bg       func() // stop function for background manager goroutine
	log      *slog.Logger
}

// Open opens a new or existing database at the given path.
// Creates the database schema if necessary.
//
// The returned [DB] is safe for concurrent use by multiple goroutines
// as long as you use a single instance throughout your program.
// Typically, you only close the DB when the program exits.
//
// The opts parameter is optional. If nil, uses default options.
func Open(path string, opts *Options) (*DB, error) {
	// Apply the default options if necessary.
	opts = applyOptions(defaultOptions, opts)
	sopts := newStoreOptions(opts)

	// Build DSN from path
	dsn := buildDSN(path, opts.DriverName, sopts.Pragma)

	// Open the database using GORM store
	sdb, err := store.Open(dsn, sopts.Dialect, sopts.Timeout)
	if err != nil {
		return nil, err
	}

	return new(sdb, opts)
}

// OpenDB connects to an existing SQL database.
// Creates the database schema if necessary.
// The opts parameter is optional. If nil, uses default options.
func OpenDB(db *sql.DB, opts *Options) (*DB, error) {
	opts = applyOptions(defaultOptions, opts)
	sopts := newStoreOptions(opts)

	// Convert sql.DB to GORM instance
	gormDB, err := gormOpen(db, sopts.Dialect)
	if err != nil {
		return nil, err
	}

	sdb, err := store.OpenDB(gormDB, sopts.Dialect, sopts.Timeout)
	if err != nil {
		return nil, err
	}
	return new(sdb, opts)
}

// new creates a new database.
func new(s *store.Store, opts *Options) (*DB, error) {
	// makeTx is called by store.Transactor on each transaction.
	// It reads dbIdx from the context via CtxDBIdx.
	makeTx := func(dialect store.Dialect, tx *gorm.DB, ctx context.Context) *Tx {
		dbIdx := store.CtxDBIdx(ctx)
		return &Tx{
			dialect: dialect,
			tx:      tx,
			hashTx:  rhash.NewTx(dialect, tx, dbIdx),
			keyTx:   rkey.NewTx(dialect, tx, dbIdx),
			listTx:  rlist.NewTx(dialect, tx, dbIdx),
			setTx:   rset.NewTx(dialect, tx, dbIdx),
			strTx:   rstring.NewTx(dialect, tx, dbIdx),
			zsetTx:  rzset.NewTx(dialect, tx, dbIdx),
		}
	}

	rdb := &DB{
		store:    s,
		act:      store.NewTransactor(s, makeTx),
		hashDB:   rhash.New(s),
		keyDB:    rkey.New(s),
		listDB:   rlist.New(s),
		setDB:    rset.New(s),
		stringDB: rstring.New(s),
		zsetDB:   rzset.New(s),
		log:      opts.Logger,
	}
	rdb.dbIdx.Store(0)
	rdb.bg = rdb.startBgManager()
	return rdb, nil
}

// WithDB returns a new DB instance with the specified database index.
// All sub-repositories are independently copied with the new index.
// This method is safe for concurrent use; each call returns an independent instance.
func (db *DB) WithDB(dbIdx int) *DB {
	newDB := &DB{
		store:    db.store,
		act:      db.act,
		hashDB:   db.hashDB.WithDB(dbIdx),
		keyDB:    db.keyDB.WithDB(dbIdx),
		listDB:   db.listDB.WithDB(dbIdx),
		setDB:    db.setDB.WithDB(dbIdx),
		stringDB: db.stringDB.WithDB(dbIdx),
		zsetDB:   db.zsetDB.WithDB(dbIdx),
		bg:       db.bg,
		log:      db.log,
	}
	newDB.dbIdx.Store(int32(dbIdx))
	return newDB
}

// Hash returns the hash repository.
// A hash (hashmap) is a field-value map associated with a key.
// Use the hash repository to work with individual hashmaps
// and their fields.
func (db *DB) Hash() *rhash.DB {
	return db.hashDB
}

// Key returns the key repository.
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
func (db *DB) Key() *rkey.DB {
	return db.keyDB
}

// List returns the list repository.
// A list is a sequence of strings ordered by insertion order.
// Use the list repository to work with lists and their elements.
func (db *DB) List() *rlist.DB {
	return db.listDB
}

// Set returns the set repository.
// A set is an unordered collection of unique strings.
// Use the set repository to work with individual sets
// and their elements, and to perform set operations.
func (db *DB) Set() *rset.DB {
	return db.setDB
}

// Str returns the string repository.
// A string is a slice of bytes associated with a key.
// Use the string repository to work with individual strings.
func (db *DB) Str() *rstring.DB {
	return db.stringDB
}

// ZSet returns the sorted set repository.
// A sorted set (zset) is a like a set, but each element has a score,
// and elements are ordered by score from low to high.
// Use the sorted set repository to work with individual sets
// and their elements, and to perform set operations.
func (db *DB) ZSet() *rzset.DB {
	return db.zsetDB
}

// Log returns the logger for the database.
func (db *DB) Log() *slog.Logger {
	return db.log
}

// Update executes a function within a writable transaction.
func (db *DB) Update(f func(tx *Tx) error) error {
	return db.act.Update(f)
}

// UpdateContext executes a function within a writable transaction.
func (db *DB) UpdateContext(ctx context.Context, f func(tx *Tx) error) error {
	return db.act.UpdateContext(ctx, f)
}

// View executes a function within a read-only transaction.
func (db *DB) View(f func(tx *Tx) error) error {
	return db.act.View(f)
}

// ViewContext executes a function within a read-only transaction.
func (db *DB) ViewContext(ctx context.Context, f func(tx *Tx) error) error {
	return db.act.ViewContext(ctx, f)
}

// Close closes the database.
// It's safe for concurrent use by multiple goroutines.
func (db *DB) Close() error {
	if db.bg != nil {
		db.bg()
	}
	return db.store.Close()
}

// startBgManager starts the goroutine that runs
// in the background and deletes expired keys.
// Triggers every 60 seconds, deletes up all expired keys.
// Returns a stop function to cleanly shut down the goroutine.
func (db *DB) startBgManager() func() {
	// TODO: needs further investigation. Deleting all keys may be expensive
	// and lead to timeouts for concurrent write operations.
	// Adaptive limits based on the number of changed keys may be a solution.
	// (see https://redis.io/docs/management/config-file/ > SNAPSHOTTING)
	// And it doesn't help that SQLite's drivers do not support DELETE LIMIT,
	// so we have to use DELETE IN (SELECT ...), which is more expensive.
	const interval = 60 * time.Second
	const nKeys = 0

	ticker := time.NewTicker(interval)
	quit := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				count, err := db.keyDB.DeleteExpired(nKeys)
				if err != nil {
					db.log.Error("bg: delete expired keys", "error", err)
				} else if count > 0 {
					db.log.Info("bg: delete expired keys", "count", count)
				}
			case <-quit:
				ticker.Stop()
				return
			}
		}
	}()
	return func() { close(quit) }
}

// Tx is a Redis-like database transaction.
// Same as [DB], Tx provides access to data structures like keys,
// strings, and hashes. The difference is that you call Tx methods
// within a transaction managed by [DB.Update] or [DB.View].
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	hashTx  *rhash.Tx
	keyTx   *rkey.Tx
	listTx  *rlist.Tx
	setTx   *rset.Tx
	strTx   *rstring.Tx
	zsetTx  *rzset.Tx
}

// Hash returns the hash transaction.
func (tx *Tx) Hash() *rhash.Tx {
	return tx.hashTx
}

// Keys returns the key transaction.
func (tx *Tx) Key() *rkey.Tx {
	return tx.keyTx
}

// List returns the list transaction.
func (tx *Tx) List() *rlist.Tx {
	return tx.listTx
}

// Set returns the set transaction.
func (tx *Tx) Set() *rset.Tx {
	return tx.setTx
}

// Str returns the string transaction.
func (tx *Tx) Str() *rstring.Tx {
	return tx.strTx
}

// ZSet returns the sorted set transaction.
func (tx *Tx) ZSet() *rzset.Tx {
	return tx.zsetTx
}

// applyOptions applies custom options to the
// default options and returns the result.
func applyOptions(opts Options, custom *Options) *Options {
	if custom == nil {
		return &opts
	}
	if custom.DriverName != "" {
		opts.DriverName = custom.DriverName
	}
	if custom.Pragma != nil {
		opts.Pragma = custom.Pragma
	}
	if custom.Timeout != 0 {
		opts.Timeout = custom.Timeout
	}
	if custom.Logger != nil {
		opts.Logger = custom.Logger
	}
	return &opts
}

// newStoreOptions creates store options from options.
// Infers the SQL dialect from the driver name.
func newStoreOptions(opts *Options) *store.Options {
	return &store.Options{
		Dialect: store.InferDialect(opts.DriverName),
		Pragma:  opts.Pragma,
		Timeout: opts.Timeout,
	}
}

// buildDSN builds a data source name from the path and driver.
func buildDSN(path string, driverName string, pragma map[string]string) string {
	switch driverName {
	case "postgres", "pgx":
		return postgresDSN(path, pragma)
	case "mysql":
		return mysqlDSN(path, pragma)
	default:
		return sqliteDSN(path, pragma)
	}
}

// sqliteDSN builds an SQLite DSN.
func sqliteDSN(path string, pragma map[string]string) string {
	if pragma == nil {
		pragma = map[string]string{
			"journal_mode": "wal",
			"synchronous":  "normal",
			"temp_store":   "memory",
			"mmap_size":    "268435456",
			"foreign_keys": "on",
		}
	}

	var ds string
	source, query, hasQuery := path, "", false
	if idx := strings.Index(path, "?"); idx != -1 {
		source = path[:idx]
		query = path[idx+1:]
		hasQuery = true
	}

	if source == ":memory:" {
		ds = "file:/redka.db"
	} else {
		ds = source
		if !strings.HasPrefix(ds, "file:") {
			ds = "file:" + ds
		}
	}

	params := make(map[string]string)
	if hasQuery {
		pairs := strings.Split(query, "&")
		for _, pair := range pairs {
			k, v, _ := strings.Cut(pair, "=")
			if k != "" {
				params[k] = v
			}
		}
	}

	params["_mutex"] = "no"
	params["_txlock"] = "immediate"

	for name, val := range pragma {
		params[name] = val
	}

	var sb strings.Builder
	first := true
	for k, v := range params {
		if !first {
			sb.WriteByte('&')
		}
		sb.WriteString(k)
		sb.WriteByte('=')
		sb.WriteString(v)
		first = false
	}

	if sb.Len() == 0 {
		return ds
	}
	return ds + "?" + sb.String()
}

// postgresDSN builds a PostgreSQL DSN.
func postgresDSN(path string, _ map[string]string) string {
	return path
}

// mysqlDSN builds a MySQL DSN.
func mysqlDSN(path string, _ map[string]string) string {
	return path
}

// gormOpen opens a GORM connection from an existing sql.DB.
func gormOpen(db *sql.DB, dialect store.Dialect) (*gorm.DB, error) {
	sqlDB, err := db.Conn(context.Background())
	if err != nil {
		return nil, err
	}

	var dialector gorm.Dialector
	switch dialect {
	case store.DialectSQLite:
		dialector = sqlite.New(sqlite.Config{Conn: sqlDB})
	case store.DialectPostgres:
		dialector = postgres.New(postgres.Config{Conn: sqlDB})
	case store.DialectMySQL:
		dialector = mysql.New(mysql.Config{Conn: sqlDB})
	default:
		return nil, store.ErrDialect
	}

	return gorm.Open(dialector, &gorm.Config{PrepareStmt: true})
}
