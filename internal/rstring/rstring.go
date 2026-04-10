// Package rstring is a database-backed string repository.
// It provides methods to interact with strings in the database.
package rstring

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DB is a database-backed string repository.
// A string is a slice of bytes associated with a key.
// Use the string repository to work with individual strings.
type DB struct {
	store  *store.Store
	update func(f func(tx *Tx) error) error
	dbIdx  int
}

// New connects to the string repository.
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

// Get returns the value of the key.
// If the key does not exist or is not a string, returns ErrNotFound.
func (d *DB) Get(key string) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Get(key)
}

// GetMany returns a map of values for given keys.
// Ignores keys that do not exist or not strings,
// and does not return them in the map.
func (d *DB) GetMany(keys ...string) (map[string]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetMany(keys...)
}

// Incr increments the integer key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an integer, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Incr(key string, delta int) (int, error) {
	var val int
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.Incr(key, delta)
		return err
	})
	return val, err
}

// IncrFloat increments the float key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an float, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) IncrFloat(key string, delta float64) (float64, error) {
	var val float64
	err := d.update(func(tx *Tx) error {
		var err error
		val, err = tx.IncrFloat(key, delta)
		return err
	})
	return val, err
}

// Set sets the key value that will not expire.
// Overwrites the value if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Set(key string, value any) error {
	err := d.update(func(tx *Tx) error {
		return tx.Set(key, value)
	})
	return err
}

// SetExpire sets the key value with an optional expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) SetExpire(key string, value any, ttl time.Duration) error {
	err := d.update(func(tx *Tx) error {
		return tx.SetExpire(key, value, ttl)
	})
	return err
}

// SetMany sets the values of multiple keys.
// Overwrites values for keys that already exist and
// creates new keys/values for keys that do not exist.
// Removes the TTL for existing keys.
// If any of the keys exists but is not a string, returns ErrKeyType.
func (d *DB) SetMany(items map[string]any) error {
	err := d.update(func(tx *Tx) error {
		return tx.SetMany(items)
	})
	return err
}

// SetWith sets the key value with additional options.
func (d *DB) SetWith(key string, value any) SetCmd {
	return SetCmd{db: d, key: key, val: value}
}

// Tx is a string repository transaction.
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	dbIdx   int
}

// NewTx creates a string repository transaction
// from a generic database transaction.
func NewTx(dialect store.Dialect, tx *gorm.DB, dbIdx int) *Tx {
	return &Tx{dialect: dialect, tx: tx, dbIdx: dbIdx}
}

// Get returns the value of the key.
// If the key does not exist or is not a string, returns ErrNotFound.
func (tx *Tx) Get(key string) (core.Value, error) {
	return tx.get(key)
}

// GetMany returns a map of values for given keys.
// Ignores keys that do not exist or not strings,
// and does not return them in the map.
func (tx *Tx) GetMany(keys ...string) (map[string]core.Value, error) {
	now := time.Now().UnixMilli()

	var results []struct {
		KeyName string
		Value   []byte
	}
	err := tx.tx.Model(&store.RString{}).
		Joins("JOIN rkey ON rstring.kid = rkey.id AND rkey.ktype = 1").
		Where("rkey.kdb = ? AND rkey.kname IN ?", tx.dbIdx, keys).
		Scopes(store.NotExpired(now)).
		Select("rkey.kname as key_name, rstring.kval as value").
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	// Fill the map with the values for existing keys.
	items := make(map[string]core.Value, len(results))
	for _, r := range results {
		items[r.KeyName] = core.Value(r.Value)
	}

	return items, nil
}

// Incr increments the integer key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an integer, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
//
// Uses SELECT FOR UPDATE to prevent race conditions in concurrent access.
func (tx *Tx) Incr(key string, delta int) (int, error) {
	now := time.Now().UnixMilli()

	var newVal int
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Lock the key row to prevent concurrent modifications (SELECT FOR UPDATE)
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, key).
			Scopes(store.NotExpired(now)).
			 Clauses(store.ForUpdate()).
			First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Key doesn't exist - create it with value = delta
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      1, // string type
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			// Create string value
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.Itoa(delta)),
			}
			return txInner.Create(&rstr).Error
		}
		if err != nil {
			return err
		}

		// Check type
		if rkey.KType != 1 {
			return core.ErrKeyType
		}

		// Get current value with lock
		var rstr store.RString
		err = txInner.Where("kid = ?", rkey.ID).First(&rstr).Error
		if err == gorm.ErrRecordNotFound {
			// No string value yet - create with delta
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.Itoa(delta)),
			}
			if err := txInner.Create(&rstr).Error; err != nil {
				return err
			}
			// Update key metadata
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
		if err != nil {
			return err
		}

		// Parse and increment
		valInt, err := strconv.Atoi(string(rstr.KVal))
		if err != nil {
			return core.ErrValueType
		}
		newVal = valInt + delta

		// Update value and key metadata atomically
		rstr.KVal = []byte(strconv.Itoa(newVal))
		if err := txInner.Save(&rstr).Error; err != nil {
			return err
		}
		return txInner.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error
	})

	return newVal, err
}

// IncrFloat increments the float key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an float, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (tx *Tx) IncrFloat(key string, delta float64) (float64, error) {
	now := time.Now().UnixMilli()

	var newVal float64
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Lock the key row to prevent concurrent modifications (SELECT FOR UPDATE)
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			// Key doesn't exist - create it with value = delta
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      1, // string type
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			// Create string value
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
			}
			return txInner.Create(&rstr).Error
		}
		if err != nil {
			return err
		}

		// Check type
		if rkey.KType != 1 {
			return core.ErrKeyType
		}

		// Get current value with lock
		var rstr store.RString
		err = txInner.Where("kid = ?", rkey.ID).First(&rstr).Error
		if err == gorm.ErrRecordNotFound {
			// No string value yet - create with delta
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
			}
			if err := txInner.Create(&rstr).Error; err != nil {
				return err
			}
			// Update key metadata
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
		if err != nil {
			return err
		}

		// Parse and increment
		valFloat, err := strconv.ParseFloat(string(rstr.KVal), 64)
		if err != nil {
			return core.ErrValueType
		}
		newVal = valFloat + delta

		// Update value and key metadata atomically
		rstr.KVal = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
		if err := txInner.Save(&rstr).Error; err != nil {
			return err
		}
		return txInner.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error
	})

	return newVal, err
}

// Set sets the key value that will not expire.
// Overwrites the value if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (tx *Tx) Set(key string, value any) error {
	return tx.SetExpire(key, value, 0)
}

// SetExpire sets the key value with an optional expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (tx *Tx) SetExpire(key string, value any, ttl time.Duration) error {
	var at time.Time
	if ttl > 0 {
		at = time.Now().Add(ttl)
	}
	err := tx.set(key, value, at)
	return err
}

// SetMany sets the values of multiple keys.
// Overwrites values for keys that already exist and
// creates new keys/values for keys that do not exist.
// Removes the TTL for existing keys.
// If any of the keys exists but is not a string, returns ErrKeyType.
//
// Optimized: uses batch queries to reduce O(N) SQL calls to O(1).
func (tx *Tx) SetMany(items map[string]any) error {
	if len(items) == 0 {
		return nil
	}

	// Convert and validate all values first
	keyNames := make([]string, 0, len(items))
	values := make(map[string][]byte, len(items))
	for k, v := range items {
		if !core.IsValueType(v) {
			return core.ErrValueType
		}
		vb, err := core.ToBytes(v)
		if err != nil {
			return err
		}
		values[k] = vb
		keyNames = append(keyNames, k)
	}

	now := time.Now().UnixMilli()

	// Batch fetch existing keys
	var existingKeys []store.RKey
	err := tx.tx.Where("kdb = ? AND kname IN ?", tx.dbIdx, keyNames).Find(&existingKeys).Error
	if err != nil {
		return err
	}

	// Build maps of existing vs new keys
	existingMap := make(map[string]store.RKey, len(existingKeys))
	for _, k := range existingKeys {
		existingMap[k.KName] = k
	}

	// Separate into existing (type=string) and new keys
	var existingKeyNames []string
	var newKeyNames []string
	for k := range items {
		if ek, ok := existingMap[k]; ok {
			if ek.KType != 1 {
				return core.ErrKeyType
			}
			existingKeyNames = append(existingKeyNames, k)
		} else {
			newKeyNames = append(newKeyNames, k)
		}
	}

	// Batch update existing keys (rkey metadata)
	if len(existingKeyNames) > 0 {
		err = tx.tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname IN ?", tx.dbIdx, existingKeyNames).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"expire_at":   nil, // clear TTL
				"modified_at": now,
				"klen":        1,
			}).Error
		if err != nil {
			return err
		}
	}

	// Batch upsert string values for existing keys using INSERT ... ON CONFLICT
	if len(existingKeyNames) > 0 {
		rstrings := make([]store.RString, 0, len(existingKeyNames))
		for _, k := range existingKeyNames {
			rstrings = append(rstrings, store.RString{
				KID:  existingMap[k].ID,
				KVal: values[k],
			})
		}
		err = tx.tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&rstrings).Error
		if err != nil {
			return err
		}
	}

	// Batch insert new keys (rkey)
	if len(newKeyNames) > 0 {
		rkeys := make([]store.RKey, 0, len(newKeyNames))
		for _, k := range newKeyNames {
			rkeys = append(rkeys, store.RKey{
				KDB:        tx.dbIdx,
				KName:      k,
				KType:      1, // string type
				KVer:       1,
				ExpireAt:   nil,
				ModifiedAt: now,
				KLen:       1,
			})
		}
		err = tx.tx.Create(&rkeys).Error
		if err != nil {
			return err
		}
	}

	// Batch insert string values for new keys
	// We need the IDs assigned to each new key, so we do a second batch fetch
	if len(newKeyNames) > 0 {
		var newKeys []store.RKey
		err = tx.tx.Where("kdb = ? AND kname IN ?", tx.dbIdx, newKeyNames).Find(&newKeys).Error
		if err != nil {
			return err
		}
		// Build name->id map for new keys
		newKeyIDs := make(map[string]int, len(newKeys))
		for _, k := range newKeys {
			newKeyIDs[k.KName] = k.ID
		}
		rstrings := make([]store.RString, 0, len(newKeyNames))
		for _, k := range newKeyNames {
			rstrings = append(rstrings, store.RString{
				KID:  newKeyIDs[k],
				KVal: values[k],
			})
		}
		err = tx.tx.Create(&rstrings).Error
		if err != nil {
			return err
		}
	}

	return nil
}

// SetWith sets the key value with additional options.
func (tx *Tx) SetWith(key string, value any) SetCmd {
	return SetCmd{tx: tx, key: key, val: value}
}

// Append appends the value to the existing string value.
// Returns the length of the string after appending.
// If the key does not exist, creates a new string with the value.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Append(key string, value []byte) (int, error) {
	var length int
	err := d.update(func(tx *Tx) error {
		var err error
		length, err = tx.Append(key, value)
		return err
	})
	return length, err
}

// Append appends the value to the existing string value.
// Returns the length of the string after appending.
func (tx *Tx) Append(key string, value []byte) (int, error) {
	// Get existing value
	existing, err := tx.get(key)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}

	// Append new value - convert to []byte to ensure type matching
	newValue := append([]byte(existing), value...)
	err = tx.update(key, newValue)
	if err != nil {
		return 0, err
	}

	return len(newValue), nil
}

// SetRange overwrites the part of the string starting at offset with the value.
// Returns the length of the string after modification.
// If the key does not exist, creates a new string filled with zeros
// (or the equivalent of setting "\x00" repeated offset times).
// If the offset is beyond the string length, fills with zeros.
func (d *DB) SetRange(key string, offset int, value []byte) (int, error) {
	var length int
	err := d.update(func(tx *Tx) error {
		var err error
		length, err = tx.SetRange(key, offset, value)
		return err
	})
	return length, err
}

// SetRange overwrites the part of the string starting at offset with the value.
func (tx *Tx) SetRange(key string, offset int, value []byte) (int, error) {
	if offset < 0 {
		return 0, core.ErrArgument
	}

	// Get existing value
	existing, err := tx.get(key)
	if err != nil && err != core.ErrNotFound {
		return 0, err
	}

	// Convert existing to []byte for proper type handling
	existingBytes := []byte(existing)

	// Extend string if offset is beyond current length
	if offset > len(existingBytes) {
		existingBytes = append(existingBytes, make([]byte, offset-len(existingBytes))...)
	}

	// Overwrite from offset
	if offset+len(value) > len(existingBytes) {
		existingBytes = append(existingBytes[:offset], value...)
	} else {
		copy(existingBytes[offset:], value)
	}

	err = tx.update(key, existingBytes)
	if err != nil {
		return 0, err
	}

	return len(existingBytes), nil
}

// GetRange returns a substring of the string value.
// Start and end are zero-based indexes.
// Negative indexes are counted from the end of the string.
// end is inclusive.
// If the key does not exist, returns an empty string.
func (d *DB) GetRange(key string, start, end int) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetRange(key, start, end)
}

// GetRange returns a substring of the string value.
func (tx *Tx) GetRange(key string, start, end int) (core.Value, error) {
	val, err := tx.get(key)
	if err != nil && err != core.ErrNotFound {
		return core.Value(nil), err
	}
	if err == core.ErrNotFound {
		return core.Value(nil), nil
	}

	// Handle negative indexes
	length := len(val)
	if start < 0 {
		start = length + start
		if start < 0 {
			start = 0
		}
	}
	if end < 0 {
		end = length + end
		if end < 0 {
			end = 0
		}
	}

	// Clamp end to length
	if end > length {
		end = length
	}

	// Handle empty range
	if start >= end || start >= length {
		return core.Value([]byte{}), nil
	}

	return val[start:end], nil
}

func (tx *Tx) get(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	var rstr store.RString
	err := tx.tx.Model(&store.RString{}).
		Joins("JOIN rkey ON rstring.kid = rkey.id AND rkey.ktype = 1").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Select("rstring.kid, rstring.kval").
		First(&rstr).Error

	if err == gorm.ErrRecordNotFound {
		return core.Value(nil), core.ErrNotFound
	}
	if err != nil {
		return core.Value(nil), err
	}
	return core.Value(rstr.KVal), nil
}

// set sets the key value and (optionally) its expiration time.
func (tx *Tx) set(key string, value any, at time.Time) error {
	valueb, err := core.ToBytes(value)
	if err != nil {
		return err
	}

	var etime *int64
	if !at.IsZero() {
		etime = new(int64)
		*etime = at.UnixMilli()
	}

	now := time.Now().UnixMilli()

	// Check if key exists and validate type
	var rkey store.RKey
	err = tx.tx.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
	if err == nil {
		// Key exists, check type
		if rkey.KType != 1 {
			return core.ErrKeyType
		}
		// Update existing key: increment version, update etime/mtime/len
		err = tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"expire_at":   etime,
				"modified_at": now,
				"klen":        1,
			}).Error
		if err != nil {
			return err
		}
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create new key
		rkey = store.RKey{
			KDB:        tx.dbIdx,
			KName:      key,
			KType:      1, // string type
			KVer:       1,
			ExpireAt:   etime,
			ModifiedAt: now,
			KLen:       1,
		}
		if err := tx.tx.Create(&rkey).Error; err != nil {
			return err
		}
	} else {
		return err
	}

	// Upsert the string value using GORM's OnConflict.
	rstr := store.RString{
		KID:  rkey.ID,
		KVal: valueb,
	}

	return tx.tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kid"}},
		DoUpdates: clause.AssignmentColumns([]string{"kval"}),
	}).Create(&rstr).Error
}

// update updates the value of the existing key without changing its
// expiration time. If the key does not exist, creates a new key with
// the specified value and no expiration time.
func (tx *Tx) update(key string, value any) error {
	valueb, err := core.ToBytes(value)
	if err != nil {
		return err
	}

	now := time.Now().UnixMilli()

	// Check if key exists and validate type
	var rkey store.RKey
	err = tx.tx.Where("kdb = ? AND kname = ?", tx.dbIdx, key).First(&rkey).Error
	if err == nil {
		// Key exists, check type
		if rkey.KType != 1 {
			return core.ErrKeyType
		}
		// Update existing key: increment version, update mtime/len (keep etime)
		err = tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        1,
			}).Error
		if err != nil {
			return err
		}
	} else if errors.Is(err, gorm.ErrRecordNotFound) {
		// Create new key (no expiration)
		rkey = store.RKey{
			KDB:        tx.dbIdx,
			KName:      key,
			KType:      1, // string type
			KVer:       1,
			ModifiedAt: now,
			KLen:       1,
		}
		if err := tx.tx.Create(&rkey).Error; err != nil {
			return err
		}
	} else {
		return err
	}

	// Upsert the string value using GORM's OnConflict.
	rstr := store.RString{
		KID:  rkey.ID,
		KVal: valueb,
	}

	return tx.tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kid"}},
		DoUpdates: clause.AssignmentColumns([]string{"kval"}),
	}).Create(&rstr).Error
}

// SetOut is the output of the Set command.
type SetOut struct {
	Prev    core.Value
	Created bool
	Updated bool
}

// SetCmd sets the key value.
type SetCmd struct {
	db          *DB
	tx          *Tx
	key         string
	val         any
	ttl         time.Duration
	at          time.Time
	keepTTL     bool
	ifExists    bool
	ifNotExists bool
}

// IfExists instructs to set the value only if the key exists.
func (c SetCmd) IfExists() SetCmd {
	c.ifExists = true
	c.ifNotExists = false
	return c
}

// IfNotExists instructs to set the value only if the key does not exist.
func (c SetCmd) IfNotExists() SetCmd {
	c.ifExists = false
	c.ifNotExists = true
	return c
}

// TTL sets the time-to-live for the value.
func (c SetCmd) TTL(ttl time.Duration) SetCmd {
	c.ttl = ttl
	c.at = time.Time{}
	c.keepTTL = false
	return c
}

// At sets the expiration time for the value.
func (c SetCmd) At(at time.Time) SetCmd {
	c.ttl = 0
	c.at = at
	c.keepTTL = false
	return c
}

// KeepTTL instructs to keep the expiration time already set for the key.
func (c SetCmd) KeepTTL() SetCmd {
	c.ttl = 0
	c.at = time.Time{}
	c.keepTTL = true
	return c
}

// Run sets the key value according to the configured options.
// Returns the previous value (if any) and the operation result
// (if the key was created or updated).
//
// Expiration time handling:
//   - If called with TTL() > 0 or At(), sets the expiration time.
//   - If called with KeepTTL(), keeps the expiration time already set for the key.
//   - If called without TTL(), At() or KeepTTL(), sets the value that will not expire.
//
// Existence checks:
//   - If called with IfExists(), sets the value only if the key exists.
//   - If called with IfNotExists(), sets the value only if the key does not exist.
//
// If the key exists but is not a string, returns ErrKeyType (unless called
// with IfExists(), in which case does nothing).
func (c SetCmd) Run() (out SetOut, err error) {
	if c.db != nil {
		var out SetOut
		err := c.db.update(func(tx *Tx) error {
			var err error
			out, err = c.run(tx)
			return err
		})
		return out, err
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return SetOut{}, nil
}

func (c SetCmd) run(tx *Tx) (out SetOut, err error) {
	if !core.IsValueType(c.val) {
		return SetOut{}, core.ErrValueType
	}

	// Get the previous value.
	prev, err := tx.get(c.key)
	if err != nil && err != core.ErrNotFound {
		return SetOut{}, err
	}
	exists := err != core.ErrNotFound

	// Set the expiration time.
	if c.ttl > 0 {
		c.at = time.Now().Add(c.ttl)
	}

	// Special cases for exists / not exists checks.
	if c.ifExists && !exists {
		// only set if the key exists
		return SetOut{Prev: prev}, nil
	}
	if c.ifNotExists && exists {
		// only set if the key does not exist
		return SetOut{Prev: prev}, nil
	}

	// Set the value.
	if c.keepTTL {
		err = tx.update(c.key, c.val)
	} else {
		err = tx.set(c.key, c.val, c.at)
	}

	if err != nil {
		return SetOut{Prev: prev}, err
	}
	return SetOut{Prev: prev, Created: !exists, Updated: exists}, nil
}
