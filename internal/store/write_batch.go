package store

import (
	"context"
	"errors"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	defaultWriteBatchWorkers = 32
	defaultWriteBatchDelay   = 250 * time.Microsecond
	defaultWriteBatchSize    = 128
	writeQueueSize           = 1024
)

var errWriteCommitterClosed = errors.New("write committer is closed")

type writeRequest struct {
	ctx     context.Context
	scope   string
	item    any
	combine func(gormTx *gorm.DB, dialect Dialect, items []any) error
	result  chan error
}

type writeCommitter struct {
	db       *gorm.DB
	dialect  Dialect
	workers  int
	channels []chan writeRequest
	wg       sync.WaitGroup
	stop     chan struct{}
	closeMu  sync.RWMutex
	closed   bool
}

func newWriteCommitter(s *Store) *writeCommitter {
	workers := positiveEnvInt("REDKA_WRITE_BATCH_WORKERS", defaultWriteBatchWorkers)
	c := &writeCommitter{
		db:       s.DB,
		dialect:  s.Dialect,
		workers:  workers,
		channels: make([]chan writeRequest, workers),
		stop:     make(chan struct{}),
	}
	for i := range c.channels {
		c.channels[i] = make(chan writeRequest, writeQueueSize)
		c.wg.Add(1)
		go c.worker(c.channels[i])
	}
	return c
}

// TransactionWrite groups callbacks for the same scope into one database
// transaction. A scope must identify one logical row or key and operation
// family; grouping unrelated callbacks could change their lock order.
func (s *Store) TransactionWrite(ctx context.Context, scope string, fn func(gormTx *gorm.DB, dialect Dialect) error) error {
	return s.TransactionWriteItems(ctx, scope, fn, func(tx *gorm.DB, dialect Dialect, items []any) error {
		return tx.Transaction(func(tx *gorm.DB) error {
			for _, item := range items {
				if err := item.(func(*gorm.DB, Dialect) error)(tx, dialect); err != nil {
					return err
				}
			}
			return nil
		})
	})
}

// TransactionWriteItems groups caller-provided items for one scope and passes
// the complete group to combine. This lets command-specific implementations
// collapse repeated updates to the same row before executing SQL.
func (s *Store) TransactionWriteItems(ctx context.Context, scope string, item any, combine func(gormTx *gorm.DB, dialect Dialect, items []any) error) error {
	if s == nil || s.DB == nil || combine == nil {
		return errors.New("invalid write transaction")
	}
	if s.inTx || s.writeCommitter == nil {
		if s.inTx {
			return combine(s.DB.WithContext(ctx), s.Dialect, []any{item})
		}
		return s.Transaction(ctx, func(tx *gorm.DB, _ Dialect) error {
			return combine(tx, s.Dialect, []any{item})
		})
	}

	req := writeRequest{
		ctx:     ctx,
		scope:   scope,
		item:    item,
		combine: combine,
		result:  make(chan error, 1),
	}

	s.writeCommitter.closeMu.RLock()
	if s.writeCommitter.closed {
		s.writeCommitter.closeMu.RUnlock()
		return errWriteCommitterClosed
	}
	index := writeScopeIndex(req.scope, s.writeCommitter.workers)
	select {
	case s.writeCommitter.channels[index] <- req:
		s.writeCommitter.closeMu.RUnlock()
	case <-ctx.Done():
		s.writeCommitter.closeMu.RUnlock()
		return ctx.Err()
	case <-s.writeCommitter.stop:
		s.writeCommitter.closeMu.RUnlock()
		return errWriteCommitterClosed
	}

	select {
	case err := <-req.result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (c *writeCommitter) worker(requests <-chan writeRequest) {
	defer c.wg.Done()
	for {
		select {
		case req := <-requests:
			c.runBatch(req, requests)
		case <-c.stop:
			c.drain(requests, errWriteCommitterClosed)
			return
		}
	}
}

func (c *writeCommitter) runBatch(first writeRequest, requests <-chan writeRequest) {
	delay := writeBatchDelay()
	maxSize := positiveEnvInt("REDKA_WRITE_BATCH_SIZE", defaultWriteBatchSize)
	batches := map[string][]writeRequest{first.scope: {first}}
	total := 1
	singletonWait := delay
	if singletonWait > 100*time.Microsecond {
		singletonWait = 100 * time.Microsecond
	}
	timer := time.NewTimer(singletonWait)
	concurrent := false
	defer timer.Stop()

collect:
	for total < maxSize {
		select {
		case req := <-requests:
			if err := req.ctx.Err(); err != nil {
				req.finish(err)
				continue
			}
			batches[req.scope] = append(batches[req.scope], req)
			total++
			if !concurrent {
				concurrent = true
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(delay)
			}
		case <-timer.C:
			break collect
		case <-c.stop:
			for scope := range batches {
				for _, req := range batches[scope] {
					req.finish(errWriteCommitterClosed)
				}
				delete(batches, scope)
			}
			c.drain(requests, errWriteCommitterClosed)
			return
		}
	}

	if !concurrent && total == 1 {
		for scope, batch := range batches {
			if err := c.execute(scope, batch); err != nil {
				batch[0].finish(err)
			} else {
				batch[0].finish(nil)
			}
		}
		return
	}

	for scope, batch := range batches {
		if err := c.execute(scope, batch); err != nil {
			for _, req := range batch {
				req.finish(err)
			}
			continue
		}
		for _, req := range batch {
			req.finish(nil)
		}
	}
}

func (c *writeCommitter) execute(scope string, batch []writeRequest) error {
	var err error
	for attempt := 0; attempt < 8; attempt++ {
		sleepBeforeRetry(attempt)
		items := make([]any, len(batch))
		for i, req := range batch {
			if err := req.ctx.Err(); err != nil {
				return err
			}
			items[i] = req.item
		}
		if len(batch) == 1 {
			err = batch[0].combine(c.db, c.dialect, items)
		} else {
			err = c.db.Transaction(func(tx *gorm.DB) error {
				return batch[0].combine(tx, c.dialect, items)
			})
		}
		if err == nil || !isRetryableTransactionError(err) {
			return err
		}
	}
	return err
}

func (c *writeCommitter) drain(requests <-chan writeRequest, err error) {
	for {
		select {
		case req := <-requests:
			req.finish(err)
		default:
			return
		}
	}
}

func (c *writeCommitter) close() {
	c.closeMu.Lock()
	if c.closed {
		c.closeMu.Unlock()
		return
	}
	c.closed = true
	close(c.stop)
	c.closeMu.Unlock()
	c.wg.Wait()
}

func (req writeRequest) finish(err error) {
	req.result <- err
}

func writeScopeIndex(scope string, workers int) int {
	if workers <= 1 {
		return 0
	}
	// Operation-specific scopes share a suffix containing only dbIdx/key.
	// Route on that suffix so all writers for one key use the same worker and
	// maintain a consistent lock order.
	if suffix := strings.LastIndexByte(scope, 0); suffix >= 0 && suffix+1 < len(scope) {
		scope = scope[suffix+1:]
	}
	var hash uint64 = 1469598103934665603
	for i := 0; i < len(scope); i++ {
		hash ^= uint64(scope[i])
		hash *= 1099511628211
	}
	return int(hash % uint64(workers))
}

func writeBatchDelay() time.Duration {
	value := os.Getenv("REDKA_WRITE_BATCH_DELAY")
	if value == "" {
		return defaultWriteBatchDelay
	}
	delay, err := time.ParseDuration(value)
	if err != nil || delay <= 0 {
		return defaultWriteBatchDelay
	}
	return delay
}

func positiveEnvInt(name string, fallback int) int {
	value, err := strconv.Atoi(os.Getenv(name))
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
