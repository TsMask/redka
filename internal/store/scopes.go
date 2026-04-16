package store

import (
	"gorm.io/gorm"
)

// NotExpired returns a GORM scope that filters keys that have not expired.
// A key is not expired if its expire_at is NULL or greater than the current time.
func NotExpired(now int64) func(db *gorm.DB) *gorm.DB {
	return func(db *gorm.DB) *gorm.DB {
		return db.Where("expire_at IS NULL OR expire_at > ?", now)
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
