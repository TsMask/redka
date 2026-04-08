package store

import (
	"context"

	"gorm.io/gorm"
)

// Transactor is a domain transaction manager.
// T is the type of the domain transaction.
type Transactor[T any] struct {
	db    *Store                    // Database handle.
	newTx func(Dialect, *gorm.DB) T // Domain transaction constructor.
}

// NewTransactor creates a new transaction manager.
func NewTransactor[T any](db *Store, newTx func(Dialect, *gorm.DB) T) *Transactor[T] {
	return &Transactor[T]{db: db, newTx: newTx}
}

// Update executes a function within a writable transaction.
func (t *Transactor[T]) Update(f func(tx T) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.db.Timeout)
	defer cancel()
	return t.UpdateContext(ctx, f)
}

// UpdateContext executes a function within a writable transaction.
func (t *Transactor[T]) UpdateContext(ctx context.Context, f func(tx T) error) error {
	return t.db.RW.WithContext(ctx).Transaction(func(gormTx *gorm.DB) error {
		tx := t.newTx(t.db.Dialect, gormTx)
		return f(tx)
	})
}

// View executes a function within a read-only transaction.
func (t *Transactor[T]) View(f func(tx T) error) error {
	ctx, cancel := context.WithTimeout(context.Background(), t.db.Timeout)
	defer cancel()
	return t.ViewContext(ctx, f)
}

// ViewContext executes a function within a read-only transaction.
func (t *Transactor[T]) ViewContext(ctx context.Context, f func(tx T) error) error {
	return t.db.RO.WithContext(ctx).Transaction(func(gormTx *gorm.DB) error {
		tx := t.newTx(t.db.Dialect, gormTx)
		return f(tx)
	})
}
