// Package rhash is a database-backed hash repository.
// It provides methods to interact with hashmaps in the database.
package rhash

import (
	"math/rand"
	"strconv"
	"context"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// HashItem represents a field-value pair in a hash.
type HashItem struct {
	Field string
	Value core.Value
}

// ScanResult represents the result of a scan operation.
type ScanResult struct {
	Cursor int
	Items  []HashItem
}

// scanPageSize is the default number
// of hash items per page when scanning.
const scanPageSize = 10

// DB is a database-backed hash repository.
// A hash (hashmap) is a field-value map associated with a key.
// Use the hash repository to work with individual hashmaps
// and their fields.
type DB struct {
	store  *store.Store
	update func(f func(tx *Tx) error) error
	dbIdx  int
}

// Tx is a hash repository transaction.
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	dbIdx   int
}

// Scanner is the iterator for hash items.
// Stops when there are no more items or an error occurs.
type Scanner struct {
	db       *Tx
	key      string
	cursor   int
	pattern  string
	pageSize int
	index    int
	cur      HashItem
	items    []HashItem
	err      error
}

// New connects to the hash repository.
// Does not create the database schema.
func New(s *store.Store) *DB {
	d := &DB{store: s, dbIdx: 0}
	newTxFn := func(dialect store.Dialect, tx *gorm.DB, ctx context.Context) *Tx {
		return NewTx(dialect, tx, store.CtxDBIdx(ctx))
	}
	d.update = store.NewTransactor(s, newTxFn).Update
	return d
}

// WithDB changes the logical database index in place and returns the same DB.
// It is safe for concurrent use; each TCP connection has its own DB instance.
func (d *DB) WithDB(dbIdx int) *DB {
	newDB := *d
	newDB.dbIdx = dbIdx
	return &newDB
}

// NewTx creates a hash repository transaction
// from a generic database transaction.
func NewTx(dialect store.Dialect, tx *gorm.DB, dbIdx int) *Tx {
	return &Tx{dialect: dialect, tx: tx, dbIdx: dbIdx}
}

// Delete deletes one or more items from a hash.
// Returns the number of fields deleted.
// Ignores non-existing fields.
// Does nothing if the key does not exist or is not a hash.
func (d *DB) Delete(key string, fields ...string) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, fields...)
		return err
	})
	return n, err
}

// Exists checks if a field exists in a hash.
// If the key does not exist or is not a hash, returns false.
func (d *DB) Exists(key, field string) (bool, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Exists(key, field)
}

// Fields returns all fields in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (d *DB) Fields(key string) ([]string, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Fields(key)
}

// Get returns the value of a field in a hash.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a hash, returns ErrNotFound.
func (d *DB) Get(key, field string) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Get(key, field)
}

// GetMany returns a map of values for given fields.
// Ignores fields that do not exist and do not return them in the map.
// If the key does not exist or is not a hash, returns an empty map.
func (d *DB) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetMany(key, fields...)
}

// Incr increments the integer value of a field in a hash.
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not an integer, returns ErrValueType.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (d *DB) Incr(key, field string, delta int) (int, error) {
	var val int
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.Incr(key, field, delta)
		return err
	})
	return val, err
}

// IncrFloat increments the float value of a field in a hash.
// Returns the value after the increment.
// If the field does not exist, sets it to 0 before the increment.
// If the field value is not a float, returns ErrValueType.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (d *DB) IncrFloat(key, field string, delta float64) (float64, error) {
	var val float64
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.IncrFloat(key, field, delta)
		return err
	})
	return val, err
}

// Items returns a map of all fields and values in a hash.
// If the key does not exist or is not a hash, returns an empty map.
func (d *DB) Items(key string) (map[string]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Items(key)
}

// Len returns the number of fields in a hash.
// If the key does not exist or is not a hash, returns 0.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Len(key)
}

// Scan iterates over hash items with fields matching pattern.
// Returns a slice of field-value pairs (see [HashItem]) of size count
// based on the current state of the cursor. Returns an empty HashItem
// slice when there are no more items.
// If the key does not exist or is not a hash, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator for hash items with fields matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a hash, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Scanner(key, pattern, pageSize)
}

// Set creates or updates the value of a field in a hash.
// Returns true if the field was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (d *DB) Set(key, field string, value any) (bool, error) {
	var created bool
	err := d.update(func(tx *Tx) error {
		var err error
		created, err = tx.Set(key, field, value)
		return err
	})
	return created, err
}

// SetMany creates or updates the values of multiple fields in a hash.
// Returns the number of fields created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (d *DB) SetMany(key string, items map[string]any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.SetMany(key, items)
		return err
	})
	return n, err
}

// SetNotExists creates the value of a field in a hash if it does not exist.
// Returns true if the field was created, false if it already exists.
// If the key does not exist, creates it.
// If the key exists but is not a hash, returns ErrKeyType.
func (d *DB) SetNotExists(key, field string, value any) (bool, error) {
	var created bool
	err := d.update(func(tx *Tx) error {
		var err error
		created, err = tx.SetNotExists(key, field, value)
		return err
	})
	return created, err
}

// Values returns all values in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (d *DB) Values(key string) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Values(key)
}

// StrLen returns the length of the value of a field in a hash.
// If the key or field does not exist, returns 0.
func (d *DB) StrLen(key, field string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.StrLen(key, field)
}

// RandField returns one or more random fields from a hash.
// If count is positive, returns up to count random fields (unique).
// If count is negative, returns |count| random fields (may contain duplicates).
// If withValues is true, returns field-value pairs; otherwise just fields.
// If the key does not exist or is not a hash, returns empty slice.
func (d *DB) RandField(key string, count int, withValues bool) ([]HashItem, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.RandField(key, count, withValues)
}

// Tx methods

// Delete deletes one or more items from a hash.
// Returns the number of fields deleted.
// Ignores non-existing fields.
// Does nothing if the key does not exist or is not a hash.
func (tx *Tx) Delete(key string, fields ...string) (int, error) {
	if len(fields) == 0 {
		return 0, core.ErrArgument
	}

	now := time.Now().UnixMilli()

	var n int64
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Find the key id
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Select("id").
			Where("kdb = ? AND kname = ? AND ktype = 4", tx.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return nil
			}
			return err
		}

		// Delete hash fields
		result := txInner.Model(&store.RHash{}).
			Where("kid = ? AND kfield IN ?", rkey.ID, fields).
			Delete(&store.RHash{})
		if result.Error != nil {
			return result.Error
		}
		n = result.RowsAffected

		if n > 0 {
			// Update the key metadata
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen - ?", n),
				}).Error
		}
		return nil
	})

	return int(n), err
}

// Exists checks if a field exists in a hash.
// If the key does not exist or is not a hash, returns false.
func (tx *Tx) Exists(key, field string) (bool, error) {
	count, err := tx.count(key, field)
	return count > 0, err
}

// Fields returns all fields in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (tx *Tx) Fields(key string) ([]string, error) {
	now := time.Now().UnixMilli()

	var results []string
	err := tx.tx.Model(&store.RHash{}).
		Select("rhash.kfield").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Pluck("kfield", &results).Error

	if err != nil {
		return nil, err
	}
	return results, nil
}

// Get returns the value of a field in a hash.
// If the key does not exist or is not a hash, returns ErrNotFound.
func (tx *Tx) Get(key, field string) (core.Value, error) {
	now := time.Now().UnixMilli()

	var rhash store.RHash
	err := tx.tx.Model(&store.RHash{}).
		Select("rhash.id, rhash.kid, rhash.kval").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", tx.dbIdx, key, field).
		Scopes(store.NotExpired(now)).
		First(&rhash).Error

	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(rhash.KVal), nil
}

// GetMany returns a map of field-value pairs for specified fields.
// Ignores non-existing fields.
func (tx *Tx) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	if len(fields) == 0 {
		return map[string]core.Value{}, nil
	}

	now := time.Now().UnixMilli()

	var results []struct {
		Field string
		Value []byte
	}
	err := tx.tx.Model(&store.RHash{}).
		Select("rhash.kfield as field, rhash.kval as value").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield IN ?", tx.dbIdx, key, fields).
		Scopes(store.NotExpired(now)).
		Find(&results).Error

	if err != nil {
		return nil, err
	}

	result := make(map[string]core.Value, len(results))
	for _, r := range results {
		result[r.Field] = core.Value(r.Value)
	}
	return result, nil
}

// Items returns all field-value pairs in a hash.
// If the key does not exist or is not a hash, returns an empty map.
func (tx *Tx) Items(key string) (map[string]core.Value, error) {
	now := time.Now().UnixMilli()

	var results []struct {
		Field string
		Value []byte
	}
	err := tx.tx.Model(&store.RHash{}).
		Select("rhash.kfield as field, rhash.kval as value").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&results).Error

	if err != nil {
		return nil, err
	}

	result := make(map[string]core.Value, len(results))
	for _, r := range results {
		result[r.Field] = core.Value(r.Value)
	}
	return result, nil
}

// Len returns the number of items in a hash.
// If the key does not exist or is not a hash, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("klen").
		Where("kdb = ? AND kname = ? AND ktype = 4", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error

	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return rkey.KLen, nil
}

// Scan iterates over hash items matching pattern.
// Returns a cursor for the next page and the items.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count <= 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()

	// Convert cursor to rowid offset
	rowid := uint64(cursor)

	var results []struct {
		ID    uint64
		Field string
		Value []byte
	}

	// Build the query with pattern matching
	query := tx.tx.Model(&store.RHash{}).
		Select("rhash.id as id, rhash.kfield as field, rhash.kval as value").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Where("rhash.id > ?", rowid)

	// Apply pattern matching
	if tx.dialect == store.DialectSQLite {
		query = query.Where("rhash.kfield GLOB ?", pattern)
	} else {
		likePattern := tx.dialect.GlobToLike(pattern)
		query = query.Where("rhash.kfield LIKE ? ESCAPE '#'", likePattern)
	}

	err := query.Order("rhash.id ASC").
		Limit(count).
		Find(&results).Error

	if err != nil {
		return ScanResult{}, err
	}

	if len(results) == 0 {
		return ScanResult{}, nil
	}

	items := make([]HashItem, len(results))
	var nextCursor uint64
	for i, r := range results {
		items[i] = HashItem{Field: r.Field, Value: core.Value(r.Value)}
		nextCursor = r.ID
	}

	return ScanResult{Cursor: int(nextCursor), Items: items}, nil
}

// Set sets the value of a field in a hash.
// Creates the hash if it does not exist.
func (tx *Tx) Set(key string, field string, value any) (bool, error) {
	now := time.Now().UnixMilli()
	valueb, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}

	var isNew bool
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Insert or update the key using GORM
		var rkey store.RKey
		err := txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      4, // hash type
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Check type and update
			if rkey.KType != 4 {
				return core.ErrKeyType
			}
			// Update version and mtime using atomic increment
			if err := txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		}

		// Get the key ID (for newly created key)
		if rkey.ID == 0 {
			err = txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
			if err != nil {
				return err
			}
		}

		// Check if this is a new field
		var existing store.RHash
		err = txInner.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error
		isNew = (err == gorm.ErrRecordNotFound)

		// Upsert the hash field
		hash := store.RHash{
			KID:    rkey.ID,
			KField: field,
			KVal:   valueb,
		}
		if err := txInner.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}, {Name: "kfield"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&hash).Error; err != nil {
			return err
		}

		// Update len if new field
		if isNew {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				UpdateColumn("klen", gorm.Expr("klen + 1")).Error
		}

		return nil
	})

	return isNew, err
}

// SetMany sets multiple field-value pairs in a hash.
// Returns the number of new fields created.
func (tx *Tx) SetMany(key string, fieldValues map[string]any) (int, error) {
	if len(fieldValues) == 0 {
		return 0, nil
	}

	now := time.Now().UnixMilli()
	newCount := 0

	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Insert or update the key
		var rkey store.RKey
		err := txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      4, // hash type
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Check type and update
			if rkey.KType != 4 {
				return core.ErrKeyType
			}
			// Update version and mtime using atomic increment
			if err := txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		}

		// Get the key ID (for newly created key)
		if rkey.ID == 0 {
			err = txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
			if err != nil {
				return err
			}
		}

		// Process each field
		for field, value := range fieldValues {
			valueb, err := core.ToBytes(value)
			if err != nil {
				return err
			}

			// Check if field exists
			var existing store.RHash
			err = txInner.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error
			if err == gorm.ErrRecordNotFound {
				newCount++
			}

			// Upsert the hash field
			hash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   valueb,
			}
			if err := txInner.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}, {Name: "kfield"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&hash).Error; err != nil {
				return err
			}
		}

		// Update len
		if newCount > 0 {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				UpdateColumn("klen", gorm.Expr("klen + ?", newCount)).Error
		}

		return nil
	})

	return newCount, err
}

// Incr increments the integer value of a field in a hash.
// Returns the value after the increment.
func (tx *Tx) Incr(key, field string, delta int) (int, error) {
	now := time.Now().UnixMilli()

	var newVal int
	var isNewField bool
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Create or update the key
		var rkey store.RKey
		err := txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      4, // hash type
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Check type and update
			if rkey.KType != 4 {
				return core.ErrKeyType
			}
			// Update version and mtime using atomic increment
			if err := txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		}

		// Get the key ID (for newly created key)
		if rkey.ID == 0 {
			err = txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
			if err != nil {
				return err
			}
		}

		// Try to get existing value
		var existing store.RHash
		err = txInner.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error

		var newValue int64
		switch err {
		case nil:
			// Parse current value
			currentVal, err := strconv.ParseInt(string(existing.KVal), 10, 64)
			if err != nil {
				return core.ErrValueType
			}
			newValue = currentVal + int64(delta)
			// Update existing
			existing.KVal = []byte(strconv.FormatInt(newValue, 10))
			if err := txInner.Save(&existing).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new field
			isNewField = true
			newValue = int64(delta)
			hash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   []byte(strconv.FormatInt(newValue, 10)),
			}
			if err := txInner.Create(&hash).Error; err != nil {
				return err
			}
		default:
			return err
		}

		newVal = int(newValue)

		// Update len if new field was created
		if isNewField {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				UpdateColumn("klen", gorm.Expr("klen + 1")).Error
		}

		return nil
	})

	return newVal, err
}

// IncrFloat increments the float value of a field in a hash.
// Returns the value after the increment.
func (tx *Tx) IncrFloat(key, field string, delta float64) (float64, error) {
	now := time.Now().UnixMilli()

	var newVal float64
	var isNewField bool
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Create or update the key
		var rkey store.RKey
		err := txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      4, // hash type
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Check type and update
			if rkey.KType != 4 {
				return core.ErrKeyType
			}
			// Update version and mtime using atomic increment
			if err := txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		}

		// Get the key ID (for newly created key)
		if rkey.ID == 0 {
			err = txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
			if err != nil {
				return err
			}
		}

		// Try to get existing value
		var existing store.RHash
		err = txInner.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error

		var newValue float64
		switch err {
		case nil:
			// Parse current value
			currentVal, err := strconv.ParseFloat(string(existing.KVal), 64)
			if err != nil {
				return core.ErrValueType
			}
			newValue = currentVal + delta
			// Update existing
			existing.KVal = []byte(strconv.FormatFloat(newValue, 'f', -1, 64))
			if err := txInner.Save(&existing).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new field
			isNewField = true
			newValue = delta
			hash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   []byte(strconv.FormatFloat(newValue, 'f', -1, 64)),
			}
			if err := txInner.Create(&hash).Error; err != nil {
				return err
			}
		default:
			return err
		}

		newVal = newValue

		// Update len if new field was created
		if isNewField {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				UpdateColumn("klen", gorm.Expr("klen + 1")).Error
		}

		return nil
	})

	return newVal, err
}

// Scanner returns an iterator for hash items with fields matching pattern.
func (tx *Tx) Scanner(key string, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// SetNotExists sets the value of a field in a hash only if the field does not exist.
// Returns true if the field was set.
func (tx *Tx) SetNotExists(key string, field string, value any) (bool, error) {
	now := time.Now().UnixMilli()
	valueb, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}

	set := false
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Insert or update the key
		var rkey store.RKey
		err := txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      4, // hash type
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		} else if err != nil {
			return err
		} else {
			// Check type
			if rkey.KType != 4 {
				return core.ErrKeyType
			}
		}

		// Get the key ID (for newly created key)
		if rkey.ID == 0 {
			err = txInner.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
			if err != nil {
				return err
			}
		}

		// Check if field exists
		var count int64
		err = txInner.Model(&store.RHash{}).
			Where("kid = ? AND kfield = ?", rkey.ID, field).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count > 0 {
			// Field already exists, do not set
			return nil
		}

		// Insert the field
		hash := store.RHash{
			KID:    rkey.ID,
			KField: field,
			KVal:   valueb,
		}
		if err := txInner.Create(&hash).Error; err != nil {
			return err
		}
		set = true

		// Update version, mtime and len
		return txInner.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen + 1"),
			}).Error
	})

	if err != nil {
		return false, err
	}

	return set, nil
}

// Values returns all values in a hash.
// If the key does not exist or is not a hash, returns an empty slice.
func (tx *Tx) Values(key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	var results [][]byte
	err := tx.tx.Model(&store.RHash{}).
		Select("rhash.kval").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Pluck("kval", &results).Error

	if err != nil {
		return nil, err
	}

	values := make([]core.Value, len(results))
	for i, v := range results {
		values[i] = core.Value(v)
	}
	return values, nil
}

// StrLen returns the length of the value of a field in a hash.
func (tx *Tx) StrLen(key, field string) (int, error) {
	val, err := tx.Get(key, field)
	if err != nil {
		return 0, err
	}
	return len(val), nil
}

// RandField returns one or more random fields from a hash.
// count > 0: return up to count unique fields
// count < 0: return |count| fields (may contain duplicates)
// withValues: include values in the result
func (tx *Tx) RandField(key string, count int, withValues bool) ([]HashItem, error) {
	// First get all fields (for small result sets we can do random selection in memory)
	fields, err := tx.Fields(key)
	if err != nil {
		return nil, err
	}
	if len(fields) == 0 {
		return []HashItem{}, nil
	}

	// If count is negative, we need to allow duplicates (with replacement)
	if count < 0 {
		count = -count
		result := make([]HashItem, count)
		for i := 0; i < count; i++ {
			idx := rand.Intn(len(fields))
			field := fields[idx]
			var value core.Value
			if withValues {
				value, _ = tx.Get(key, field)
			}
			result[i] = HashItem{Field: field, Value: value}
		}
		return result, nil
	}

	// For positive count (unique fields), shuffle and take up to count
	if len(fields) <= count {
		// Return all fields
		result := make([]HashItem, len(fields))
		for i, field := range fields {
			var value core.Value
			if withValues {
				value, _ = tx.Get(key, field)
			}
			result[i] = HashItem{Field: field, Value: value}
		}
		// Shuffle the result
		for i := len(result) - 1; i > 0; i-- {
			j := rand.Intn(i + 1)
			result[i], result[j] = result[j], result[i]
		}
		return result, nil
	}

	// Fisher-Yates shuffle and take first count
	for i := len(fields) - 1; i > 0; i-- {
		j := rand.Intn(i + 1)
		fields[i], fields[j] = fields[j], fields[i]
	}
	result := make([]HashItem, count)
	for i := 0; i < count; i++ {
		field := fields[i]
		var value core.Value
		if withValues {
			value, _ = tx.Get(key, field)
		}
		result[i] = HashItem{Field: field, Value: value}
	}
	return result, nil
}

// Helper methods

func (tx *Tx) count(key string, field string) (int, error) {
	now := time.Now().UnixMilli()

	var count int64
	err := tx.tx.Model(&store.RHash{}).
		Select("COUNT(rhash.kfield)").
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = 4").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", tx.dbIdx, key, field).
		Scopes(store.NotExpired(now)).
		Count(&count).Error

	if err != nil {
		return 0, err
	}
	return int(count), nil
}

// Scanner methods

func newScanner(db *Tx, key string, pattern string, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       db,
		key:      key,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
		index:    0,
		items:    []HashItem{},
	}
}

// Scan advances to the next item, fetching items from db as necessary.
// Returns false when there are no more items or an error occurs.
// Returns false if the key does not exist or is not a hash.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.items) {
		// Fetch a new page of items.
		result, err := sc.db.Scan(sc.key, sc.cursor, sc.pattern, sc.pageSize)
		if err != nil {
			sc.err = err
			return false
		}
		sc.cursor = result.Cursor
		sc.items = result.Items
		sc.index = 0
		if len(sc.items) == 0 {
			return false
		}
	}
	// Advance to the next item from the current page.
	sc.cur = sc.items[sc.index]
	sc.index++
	return true
}

// Item returns the current hash item.
func (sc *Scanner) Item() HashItem {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}
