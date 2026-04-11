package store

import (
	"net/url"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// newPostgres creates a new Postgres database handle using GORM.
func newPostgres(dsn string) (*Store, error) {
	dsn = postgresDataSource(dsn)

	// Open the database connection
	db, err := gorm.Open(postgres.Open(dsn), gormConfig())
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect: DialectPostgres,
		DB:      db,
	}

	// Configure connection pool
	if err := store.configurePoolPostgres(); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolPostgres sets the number of connections for Postgres.
func (s *Store) configurePoolPostgres() error {
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

// postgresDataSource returns a Postgres connection string
func postgresDataSource(path string) string {
	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	// Apply additional settings.
	// for name, val := range pragma {
	// 	params.Add(name, val)
	// }

	// Return the connection string.
	if len(params) == 0 {
		return source
	}
	return source + "?" + params.Encode()
}
