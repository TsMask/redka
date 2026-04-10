package store

import (
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NotExpired returns a GORM scope that filters keys that have not expired.
// A key is not expired if its expire_at is NULL or greater than the current time.
func NotExpired(now int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("expire_at IS NULL OR expire_at > ?", now)
	}
}

// LimitAll returns a GORM scope that removes the default limit for a dialect.
// Different databases have different ways to express "no limit":
//   - SQLite: LIMIT -1
//   - PostgreSQL: LIMIT ALL
//   - MySQL: LIMIT 18446744073709551615 (max uint64)
func LimitAll(dialect Dialect) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		switch dialect {
		case DialectSQLite:
			return db.Limit(-1)
		case DialectPostgres:
			return db.Limit(-1) // GORM handles LIMIT ALL internally
		case DialectMySQL:
			// MySQL uses max int value for "no limit"
			// Note: GORM's Limit takes int, so we use a large int value
			return db.Limit(2147483647) // MaxInt32 for cross-platform compatibility
		default:
			return db
		}
	}
}

// GlobPattern returns a GORM scope that filters keys matching a glob pattern.
// SQLite uses GLOB operator natively.
// PostgreSQL and MySQL convert glob pattern to LIKE pattern.
func GlobPattern(dialect Dialect, pattern string) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		if dialect == DialectSQLite {
			// SQLite supports GLOB natively
			return db.Where("kname GLOB ?", pattern)
		}
		// PostgreSQL and MySQL use LIKE with converted pattern
		likePattern := dialect.GlobToLike(pattern)
		return db.Where("kname LIKE ? ESCAPE '#'", likePattern)
	}
}

// ElemPattern returns a GORM clause for matching element patterns (bytea/text).
// SQLite uses GLOB natively.
// PostgreSQL casts bytea to text for LIKE compatibility.
// MySQL uses LIKE directly on VARBINARY.
func ElemPattern(dialect Dialect, column string, pattern string) clause.Expression {
	likePattern := dialect.GlobToLike(pattern)
	if dialect == DialectSQLite {
		return clause.Expr{SQL: column + " GLOB ?", Vars: []any{pattern}}
	}
	if dialect == DialectPostgres {
		// PostgreSQL: cast bytea to text for LIKE compatibility
		return clause.Expr{SQL: "encode(" + column + ", 'escape')::text LIKE ? ESCAPE '#'", Vars: []any{likePattern}}
	}
	// MySQL: VARBINARY supports LIKE directly
	return clause.Expr{SQL: column + " LIKE ? ESCAPE '#'", Vars: []any{likePattern}}
}

// RandomOrder returns a GORM scope that orders results randomly.
// Different databases have different random functions:
//   - SQLite: RANDOM()
//   - PostgreSQL: RANDOM()
//   - MySQL: RAND()
func RandomOrder(dialect Dialect) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		switch dialect {
		case DialectSQLite, DialectPostgres:
			return db.Order("RANDOM()")
		case DialectMySQL:
			return db.Order("RAND()")
		default:
			return db
		}
	}
}

// ForUpdate returns a GORM clause for row-level locking (SELECT FOR UPDATE).
// This prevents race conditions in concurrent read-modify-write operations.
// For SQLite, this is a no-op because SQLite uses database-level locking
// via BEGIN IMMEDIATE transactions (configured in sqliteDataSource).
func ForUpdate() clause.Locking {
	return clause.Locking{
		Strength: "UPDATE",
		Options:  "NOWAIT",
	}
}
