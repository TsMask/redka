// Package rkey is a database-backed key repository.
// It provides methods to interact with keys in the database.
package rkey

import (
	"context"
	"errors"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
)

// scanPageSize is the default number
// of keys per page when scanning.
const scanPageSize = 10

// ScanResult represents a result of the Scan call.
type ScanResult struct {
	Cursor int
	Keys   []core.Key
}

// DB is a database-backed key repository.
// A key is a unique identifier for a data structure
// (string, list, hash, etc.). Use the key repository
// to manage all keys regardless of their type.
//
// This is a simplified architecture that directly uses store.Store
// without additional transaction wrappers. Each method handles
// its own transactions internally when needed.
type DB struct {
	store *store.Store
	dbIdx int
}

// Scanner is the iterator for keys.
// Stops when there are no more keys or an error occurs.
type Scanner struct {
	db       *DB
	cursor   int
	pattern  string
	ktype    core.TypeID
	pageSize int
	index    int
	cur      core.Key
	keys     []core.Key
	err      error
}

// New creates a new database-backed key repository.
// Does not create the database schema.
func New(s *store.Store) *DB {
	return &DB{store: s, dbIdx: 0}
}

// WithDB changes the logical database index in place and returns the same DB.
// It is safe for concurrent use; each TCP connection has its own DB instance.
func (d *DB) WithDB(dbIdx int) *DB {
	newDB := *d
	newDB.dbIdx = dbIdx
	return &newDB
}

// Count returns the number of existing keys among specified.
func (d *DB) Count(keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	var count int64
	err := d.store.DB.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ?", d.dbIdx, keys).
		Scopes(store.NotExpired(now)).
		Count(&count).Error
	return int(count), err
}

// Delete deletes keys and their values, regardless of the type.
// Returns the number of deleted keys. Non-existing keys are ignored.
func (d *DB) Delete(keys ...string) (int, error) {
	now := time.Now().UnixMilli()
	result := d.store.DB.Where("kdb = ? AND kname IN ?", d.dbIdx, keys).
		Scopes(store.NotExpired(now)).
		Delete(&store.RKey{})
	if result.Error != nil {
		return 0, result.Error
	}
	return int(result.RowsAffected), nil
}

// DeleteAll deletes all keys and their values, effectively resetting
// the database. Should not be run inside a database transaction.
func (d *DB) DeleteAll() error {
	if d.store.Dialect == store.DialectPostgres {
		return d.store.DB.Exec("TRUNCATE TABLE rkey CASCADE").Error
	}
	return d.store.DB.Where("1 = 1").Delete(&store.RKey{}).Error
}

// DeleteExpired deletes keys with expired TTL, but no more than n keys.
// If n = 0, deletes all expired keys.
func (d *DB) DeleteExpired(n int) (count int, err error) {
	now := time.Now().UnixMilli()

	expiredScope := func(db *gorm.DB) *gorm.DB {
		return db.Where("kdb = ? AND expire_at IS NOT NULL AND expire_at <= ?", d.dbIdx, now)
	}

	if n > 0 {
		var expiredIDs []int
		err := d.store.DB.Model(&store.RKey{}).
			Scopes(expiredScope).
			Limit(n).
			Pluck("id", &expiredIDs).Error
		if err != nil {
			return 0, err
		}
		if len(expiredIDs) == 0 {
			return 0, nil
		}
		// Use transaction to ensure delete operations are isolated
		err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
			result := tx.Where("id IN ?", expiredIDs).Delete(&store.RKey{})
			if result.Error != nil {
				return result.Error
			}
			count = int(result.RowsAffected)
			return nil
		})
		return count, err
	}

	result := d.store.DB.Scopes(expiredScope).Delete(&store.RKey{})
	if result.Error != nil {
		return 0, result.Error
	}
	return int(result.RowsAffected), nil
}

// Exists reports whether the key exists.
func (d *DB) Exists(key string) (bool, error) {
	count, err := d.Count(key)
	return count > 0, err
}

// Expire sets a time-to-live (ttl) for the key using a relative duration.
// After the ttl passes, the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Expire(key string, ttl time.Duration) error {
	at := time.Now().Add(ttl)
	return d.ExpireAt(key, at)
}

// ExpireAt sets an expiration time for the key. After this time,
// the key is expired and no longer exists.
// If the key does not exist, returns ErrNotFound.
func (d *DB) ExpireAt(key string, at time.Time) error {
	etime := at.UnixMilli()

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		result := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Updates(map[string]any{
				"kver":      gorm.Expr("kver + 1"),
				"expire_at": etime,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return core.ErrNotFound
		}
		return nil
	})
}

// Get returns a specific key with all associated details.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Get(key string) (core.Key, error) {
	now := time.Now().UnixMilli()
	var rkey store.RKey
	err := d.store.DB.Where("kdb = ? AND kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Key{}, core.ErrNotFound
	}
	if err != nil {
		return core.Key{}, err
	}
	return core.Key{
		ID:      rkey.ID,
		Key:     rkey.KName,
		Type:    core.TypeID(rkey.KType),
		Version: rkey.KVer,
		ETime:   rkey.ExpireAt,
		MTime:   rkey.ModifiedAt,
		Len:     rkey.KLen,
	}, nil
}

// Keys returns all keys matching pattern.
// Supports glob-style patterns like these:
//
//	key*  k?y  k[bce]y  k[!a-c][y-z]
//
// Use this method only if you are sure that the number of keys is
// limited. Otherwise, use the [DB.Scan] or [DB.Scanner] methods.
func (d *DB) Keys(pattern string) ([]core.Key, error) {
	now := time.Now().UnixMilli()

	var rkeys []store.RKey
	err := d.store.DB.Where("kdb = ?", d.dbIdx).
		Scopes(store.NotExpired(now), store.GlobPattern(d.store.Dialect, pattern)).
		Find(&rkeys).Error
	if err != nil {
		return nil, err
	}

	keys := make([]core.Key, len(rkeys))
	for i, rkey := range rkeys {
		keys[i] = core.Key{
			ID:      rkey.ID,
			Key:     rkey.KName,
			Type:    core.TypeID(rkey.KType),
			Version: rkey.KVer,
			ETime:   rkey.ExpireAt,
			MTime:   rkey.ModifiedAt,
			Len:     rkey.KLen,
		}
	}
	return keys, nil
}

// Len returns the total number of keys, including expired ones.
func (d *DB) Len() (int, error) {
	var count int64
	err := d.store.DB.Model(&store.RKey{}).
		Where("kdb = ?", d.dbIdx).
		Count(&count).Error
	return int(count), err
}

// Persist removes the expiration time for the key.
// If the key does not exist, returns ErrNotFound.
func (d *DB) Persist(key string) error {
	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		result := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Updates(map[string]any{
				"kver":      gorm.Expr("kver + 1"),
				"expire_at": nil,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return core.ErrNotFound
		}
		return nil
	})
}

// Random returns a random key.
// If there are no keys, returns ErrNotFound.
func (d *DB) Random() (core.Key, error) {
	now := time.Now().UnixMilli()
	var rkey store.RKey
	err := d.store.DB.Where("kdb = ?", d.dbIdx).
		Scopes(store.NotExpired(now), store.RandomOrder(d.store.Dialect)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Key{}, core.ErrNotFound
	}
	if err != nil {
		return core.Key{}, err
	}
	return core.Key{
		ID:      rkey.ID,
		Key:     rkey.KName,
		Type:    core.TypeID(rkey.KType),
		Version: rkey.KVer,
		ETime:   rkey.ExpireAt,
		MTime:   rkey.ModifiedAt,
		Len:     rkey.KLen,
	}, nil
}

// Rename changes the key name.
// If there is an existing key with the new name, it is replaced.
// If the old key does not exist, returns ErrNotFound.
func (d *DB) Rename(key, newKey string) error {
	if key == newKey {
		return nil
	}

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var oldK store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&oldK).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		var newK store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, newKey).
			Scopes(store.NotExpired(now)).
			First(&newK).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil && oldK.KType != newK.KType {
			return core.ErrKeyType
		}

		if err == nil {
			if err := tx.Where("id = ?", newK.ID).Delete(&store.RKey{}).Error; err != nil {
				return err
			}
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", oldK.ID).
			Updates(map[string]any{
				"kname":       newKey,
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error
	})
}

// RenameNotExists changes the key name.
// If there is an existing key with the new name, does nothing.
// Returns true if the key was renamed, false otherwise.
func (d *DB) RenameNotExists(key, newKey string) (bool, error) {
	if key == newKey {
		return false, nil
	}

	var renamed bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var oldK store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&oldK).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		var newK store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, newKey).
			Scopes(store.NotExpired(now)).
			First(&newK).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			return nil
		}

		err = tx.Model(&store.RKey{}).
			Where("id = ?", oldK.ID).
			Updates(map[string]any{
				"kname":       newKey,
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error
		if err != nil {
			return err
		}
		renamed = true
		return nil
	})
	return renamed, err
}

// Scan iterates over keys matching pattern.
// Returns a slice of keys (see [core.Key]) of size count
// based on the current state of the cursor.
// Returns an empty slice when there are no more keys.
//
// Filtering and limiting options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - count to limit the number of keys returned (0 = default).
func (d *DB) Scan(cursor int, pattern string, ktype core.TypeID, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}
	now := time.Now().UnixMilli()
	var rkeys []store.RKey
	db := d.store.DB.Where("kdb = ? AND id > ?", d.dbIdx, cursor).
		Scopes(store.NotExpired(now), store.GlobPattern(d.store.Dialect, pattern)).
		Order("id ASC").
		Limit(count)
	if ktype != core.TypeAny {
		db = db.Where("ktype = ?", int(ktype))
	}
	err := db.Find(&rkeys).Error
	if err != nil {
		return ScanResult{}, err
	}

	keys := make([]core.Key, len(rkeys))
	for i, rkey := range rkeys {
		keys[i] = core.Key{
			ID:      rkey.ID,
			Key:     rkey.KName,
			Type:    core.TypeID(rkey.KType),
			Version: rkey.KVer,
			ETime:   rkey.ExpireAt,
			MTime:   rkey.ModifiedAt,
			Len:     rkey.KLen,
		}
	}

	maxID := 0
	if len(keys) > 0 {
		maxID = keys[len(keys)-1].ID
	}

	return ScanResult{maxID, keys}, nil
}

// Scanner returns an iterator for keys matching pattern.
// The scanner returns keys one by one, fetching them
// from the database in pageSize batches when necessary.
// Stops when there are no more items or an error occurs.
//
// Filtering and pagination options:
//   - pattern (glob-style) to filter keys by name (* = any name).
//   - ktype to filter keys by type (TypeAny = any type).
//   - pageSize to limit the number of keys fetched at once (0 = default).
func (d *DB) Scanner(pattern string, ktype core.TypeID, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       d,
		cursor:   0,
		pattern:  pattern,
		ktype:    ktype,
		pageSize: pageSize,
		index:    0,
		keys:     []core.Key{},
	}
}

// Scan advances to the next key, fetching keys from db as necessary.
// Returns false when there are no more keys or an error occurs.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.keys) {
		result, err := sc.db.Scan(sc.cursor, sc.pattern, sc.ktype, sc.pageSize)
		if err != nil {
			sc.err = err
			return false
		}
		sc.cursor = result.Cursor
		sc.keys = result.Keys
		sc.index = 0
		if len(sc.keys) == 0 {
			return false
		}
	}
	sc.cur = sc.keys[sc.index]
	sc.index++
	return true
}

// Key returns the current key.
func (sc *Scanner) Key() core.Key {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}
