package store

import (
	"strings"

	"github.com/tsmask/redka/internal/core"
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

// ConstraintFailed checks if the error is due to
// a constraint violation on a column.
// Error examples:
//   - sqlite3.Error (NOT NULL constraint failed: rkey.type)
//   - *pq.Error (pq: null value in column "type" of relation "rkey" violates not-null constraint)
//   - *mysql.MySQLError (Error 1048: Column 'type' cannot be null)
func (d Dialect) ConstraintFailed(err error, constraint, table string, column string) bool {
	var message string
	switch d {
	case DialectPostgres:
		message = `"` + column + `" of relation "` + table +
			`" violates ` + strings.ReplaceAll(constraint, " ", "-") + ` constraint`
	case DialectSQLite:
		message = constraint + " constraint failed: " + table + "." + column
	case DialectMySQL:
		// MySQL error messages differ by constraint type
		if constraint == "not null" {
			message = "column '" + column + "' cannot be null"
		} else {
			message = constraint + " constraint failed"
		}
	}
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, strings.ToLower(message))
}

// TypedError returns ErrKeyType if the error is due to a not-null
// constraint violation on rkey.ktype.
// Otherwise, returns the original error.
func (d Dialect) TypedError(err error) error {
	if err == nil {
		return nil
	}
	if d.ConstraintFailed(err, "not null", "rkey", "ktype") {
		return core.ErrKeyType
	}
	return err
}
