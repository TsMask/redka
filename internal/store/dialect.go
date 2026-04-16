package store

import (
	"strings"
)

// SQL dialect.
type Dialect string

const (
	DialectSQLite   Dialect = "sqlite"
	DialectPostgres Dialect = "postgres"
	DialectMySQL    Dialect = "mysql"
	DialectUnknown  Dialect = "unknown"
)

// GlobToLike creates a like-style pattern from a glob-style pattern.
// Only supports * and ? wildcards, not [abc] and [!abc].
// Escapes % and _ special characters with # (e.g., #%, #_, ##).
// SQLite supports glob-style patterns natively.
func (d Dialect) GlobToLike(pattern string) string {
	if d == DialectSQLite {
		// SQLite supports glob-style patterns.
		return pattern
	}
	var b strings.Builder
	for _, char := range pattern {
		switch char {
		case '*':
			b.WriteByte('%')
		case '?':
			b.WriteByte('_')
		case '%', '_', '#':
			b.WriteByte('#')
			b.WriteRune(char)
		default:
			b.WriteRune(char)
		}
	}
	return b.String()
}
