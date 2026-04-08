package store

import (
	"net/url"
	"strings"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// newPostgres creates a new Postgres database handle using GORM.
func newPostgres(dsn string, opts *Options) (*Store, error) {
	// Build RW and RO data sources
	rwDSN := postgresDataSource(dsn, false, opts.Pragma)
	roDSN := postgresDataSource(dsn, true, opts.Pragma)

	// Open RW connection
	rwDB, err := gorm.Open(postgres.Open(rwDSN), gormConfig())
	if err != nil {
		return nil, err
	}

	// Open RO connection
	roDB, err := gorm.Open(postgres.Open(roDSN), gormConfig())
	if err != nil {
		return nil, err
	}

	store := &Store{
		Dialect:        DialectPostgres,
		RW:             rwDB,
		RO:             roDB,
		Timeout:        opts.Timeout,
		MaxPoolConns:   opts.MaxPoolConns,
		MinPoolConns:   opts.MinPoolConns,
	}

	// Configure connection pools
	if err := store.configurePoolsPostgres(opts.ReadOnly); err != nil {
		return nil, err
	}

	return store, nil
}

// configurePoolsPostgres sets the number of connections for Postgres.
func (s *Store) configurePoolsPostgres(readOnly bool) error {
	maxConns := s.MaxPoolConns
	if maxConns == 0 {
		maxConns = suggestNumConns()
	}
	minIdle := s.MinPoolConns
	if minIdle == 0 {
		minIdle = 2
	}

	roSqlDB, err := s.RO.DB()
	if err != nil {
		return err
	}
	configurePool(roSqlDB, maxConns, minIdle)

	if !readOnly {
		rwSqlDB, err := s.RW.DB()
		if err != nil {
			return err
		}
		configurePool(rwSqlDB, maxConns, minIdle)
	}

	return nil
}

// postgresDataSource returns a Postgres connection string
// for a read-only or read-write mode.
func postgresDataSource(path string, readOnly bool, pragma map[string]string) string {
	// Parse the parameters.
	source, query, _ := strings.Cut(path, "?")
	params, _ := url.ParseQuery(query)

	// Set the connection mode (writable or read-only).
	if readOnly {
		params.Set("default_transaction_read_only", "on")
	}

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
