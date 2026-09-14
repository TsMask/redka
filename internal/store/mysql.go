package store

import (
	"strings"
	"time"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

// newMySQL creates a new MySQL database handle using GORM.
func newMySQL(dsn string) (*Store, error) {
	dsn = mysqlDataSource(dsn)

	// Open the database connection
	// OceanBase's MySQL mode limits open cursors. GORM's prepared-statement
	// cache keeps a cursor per statement/connection; interpolation already
	// avoids prepare round trips for dynamic statements.
	db, err := gorm.Open(mysql.Open(dsn), gormConfigWithPreparedStatements(false))
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect: DialectMySQL,
		DB:      db,
	}

	// Configure connection pool
	if err := store.configurePool(); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePool sets the number of connections for MySQL.
func (s *Store) configurePool() error {
	sqlDB, err := s.DB.DB()
	if err != nil {
		return err
	}
	sqlDB.SetMaxOpenConns(64)
	sqlDB.SetMaxIdleConns(100)
	sqlDB.SetConnMaxLifetime(30 * time.Minute)
	sqlDB.SetConnMaxIdleTime(5 * time.Minute)
	return nil
}

// mysqlDataSource returns a MySQL connection string with appropriate settings.
// MySQL DSN format: [username[:password]@][protocol[(address)]]/dbname[?param1=value1&...&paramN=valueN]
func mysqlDataSource(dsn string) string {
	// Parse the parameters
	source, query, hasQuery := strings.Cut(dsn, "?")

	// Build parameters map from existing query string
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

	// Set required MySQL parameters
	// charset=utf8mb4 for proper Unicode support
	if _, ok := params["charset"]; !ok {
		params["charset"] = "utf8mb4"
	}

	// parseTime=True to scan TIME/DATE/DATETIME into time.Time
	if _, ok := params["parseTime"]; !ok {
		params["parseTime"] = "True"
	}

	// loc=Local for local timezone
	if _, ok := params["loc"]; !ok {
		params["loc"] = "Local"
	}

	// CLIENT_FOUND_ROWS keeps matched, unchanged updates on the one-statement
	// hot path instead of falling back to a read plus transaction.
	if _, ok := params["clientFoundRows"]; !ok {
		params["clientFoundRows"] = "true"
	}

	// Interpolation avoids an extra prepare round trip for dynamic writes.
	if _, ok := params["interpolateParams"]; !ok {
		params["interpolateParams"] = "true"
	}

	// Build the query string
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
		return source
	}
	return source + "?" + sb.String()
}
