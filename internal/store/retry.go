package store

import (
	"context"
	"errors"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/gorm"
)

// TransactionRetry retries transient SQL serialization failures for repository
// operations whose callback has no externally visible side effects.
func (s *Store) TransactionRetry(ctx context.Context, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		sleepBeforeRetry(attempt)
		if err = s.Transaction(ctx, fn); err == nil || !isRetryableTransactionError(err) {
			return err
		}
	}
	return err
}

// Retry retries a transient failure in a callback that executes directly on the
// connection, such as a one-statement hot-path update.
func (s *Store) Retry(ctx context.Context, fn func(db *gorm.DB, dialect Dialect) error) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		sleepBeforeRetry(attempt)
		if err = fn(s.DB.WithContext(ctx), s.Dialect); err == nil || !isRetryableTransactionError(err) {
			return err
		}
	}
	return err
}

func sleepBeforeRetry(attempt int) {
	if attempt == 0 {
		return
	}
	delay := int64(10) << attempt
	if delay > 320 {
		delay = 320
	}
	time.Sleep(time.Duration(delay) * time.Millisecond)
}

func isRetryableTransactionError(err error) bool {
	var mysqlErr *mysql.MySQLError
	if errors.As(err, &mysqlErr) {
		return mysqlErr.Number == 1213 || mysqlErr.Number == 1205
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code == "40P01" || pgErr.Code == "40001"
	}
	return false
}
