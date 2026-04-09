package store

import (
	"net/url"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// newPostgres creates a new Postgres database handle using GORM.
func newPostgres(dsn string, opts *Options) (*Store, error) {
	dsn = postgresDataSource(dsn, opts.Pragma)

	// Open the database connection
	db, err := gorm.Open(postgres.Open(dsn), gormConfig())
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect:      DialectPostgres,
		DB:           db,
		Timeout:      opts.Timeout,
		MaxPoolConns: opts.MaxPoolConns,
		MinPoolConns: opts.MinPoolConns,
	}

	// Configure connection pool
	if err := store.configurePoolPostgres(); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolPostgres sets the number of connections for Postgres.
func (s *Store) configurePoolPostgres() error {
	maxConns := s.MaxPoolConns
	if maxConns == 0 {
		maxConns = suggestNumConns()
	}
	minIdle := s.MinPoolConns
	if minIdle == 0 {
		minIdle = 2
	}

	sqlDB, err := s.DB.DB()
	if err != nil {
		return err
	}
	configurePool(sqlDB, maxConns, minIdle)

	return nil
}

// postgresDataSource returns a Postgres connection string
func postgresDataSource(path string, pragma map[string]string) string {
	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	// Apply additional settings.
	for name, val := range pragma {
		params.Add(name, val)
	}

	// Return the connection string.
	if len(params) == 0 {
		return source
	}
	return source + "?" + params.Encode()
}
