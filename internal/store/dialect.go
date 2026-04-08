package store

import (
	"errors"
	"strconv"
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

// ErrDialect is returned when an unsupported dialect is used.
var ErrDialect = errors.New("unknown SQL dialect")

// Enumerate replaces ? placeholders with $1, $2, ... $n for Postgres.
// SQLite and MySQL use ? placeholders natively.
func (d Dialect) Enumerate(query string) string {
	if d != DialectPostgres {
		// SQLite and MySQL support ? placeholders.
		return query
	}
	// Replace ? with $1, $2, ... $n placeholders for Postgres.
	var b strings.Builder
	var phIdx int
	for _, char := range query {
		if char == '?' {
			phIdx++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(phIdx))
		} else {
			b.WriteRune(char)
		}
	}
	return b.String()
}

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

// LimitAll returns a SQL query fragment to limit the result to all rows.
func (d Dialect) LimitAll() string {
	switch d {
	case DialectSQLite:
		return "limit -1"
	case DialectPostgres:
		return "limit all"
	case DialectMySQL:
		// MySQL uses a very large number instead of 'all'
		return "limit 18446744073709551615"
	default:
		return ""
	}
}

// TimeNow returns the SQL expression for current time in milliseconds.
func (d Dialect) TimeNow() string {
	switch d {
	case DialectSQLite:
		return "unixepoch('subsec') * 1000"
	case DialectPostgres:
		return "(extract(epoch from now()) * 1000)::bigint"
	case DialectMySQL:
		return "(UNIX_TIMESTAMP(NOW(3)) * 1000)"
	default:
		return ""
	}
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

// InferDialect infers the SQL dialect from the driver name.
func InferDialect(driverName string) Dialect {
	switch {
	case driverName == "postgres" || driverName == "pgx":
		return DialectPostgres
	case strings.HasPrefix(driverName, "sqlite"):
		return DialectSQLite
	case driverName == "mysql":
		return DialectMySQL
	default:
		return DialectUnknown
	}
}
