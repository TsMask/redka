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

	return store, nil
}

// configurePoolSQLite sets the number of connections for SQLite.
// SQLite uses database-level locking for writes, so we limit the number of
// concurrent connections to reduce lock contention and improve performance.
func (s *Store) configurePoolSQLite() error {
	sqlDB, err := s.DB.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(2)
	sqlDB.SetConnMaxLifetime(30 * time.Minute) // Prevent stale connections
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)  // Reclaim idle connections
	return nil
}

// sqliteDataSource returns an SQLite connection string.
func sqliteDataSource(path string) string {
	// Parse the parameters.
	source, query, hasQuery := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)
	if !hasQuery {
		// Apply the pragma settings.
		pragmas := []string{
			"journal_mode(WAL)",            // 启用 WAL 模式
			"synchronous(NORMAL)",          // NORMAL 同步级别
			"wal_autocheckpoint(5000)",     // 每 5000 页触发一次检查点
			"busy_timeout(5000)",           // 设置忙碌时最大等待 5 秒
			"mmap_size(268435456)",         // 设置 MMAP 大小为 256MB
			"locking_mode(NORMAL)",         // 使用 NORMAL 锁定模式
			"journal_size_limit(16777216)", // 设置 WAL 日志文件大小为 16MB
			"cache_size(-64000)",           // 设置缓存大小为 64MB (-64000 表示 64MB)
			"temp_store(MEMORY)",           // 使用内存存储临时表
			"page_size=4096",               // 设置页大小为 4KB
			"foreign_keys=ON",              // 启用外键约束
		}
		for _, v := range pragmas {
			params.Add("_pragma", v)
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
