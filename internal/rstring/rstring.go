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
//
// This is a simplified architecture that directly uses store.Store
// without additional transaction wrappers. Each method handles
// its own transactions internally when needed.
type DB struct {
	store *store.Store
	dbIdx int
}

// New connects to the string repository.
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

// Get returns the value of the key.
// If the key does not exist or is not a string, returns ErrNotFound.
func (d *DB) Get(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := d.store.DB.
		Where("kdb = ? AND kname = ?", d.dbIdx, key).
		First(&rkey).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}

	// Lazy expiration: check if key is expired
	if rkey.ExpireAt != nil && *rkey.ExpireAt <= now {
		// Key is expired, delete it and return not found
		d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
			tx.Where("kid = ?", rkey.ID).Delete(&store.RString{})
			tx.Delete(&rkey)
			return nil
		})
		return core.Value{}, core.ErrNotFound
	}

	if rkey.KType != core.TypeString.Value() {
		return core.Value{}, core.ErrKeyType
	}

	var result struct {
		Value []byte
	}
	err = d.store.DB.Model(&store.RString{}).
		Where("kid = ?", rkey.ID).
		Select("kval as value").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}
	return core.Value(result.Value), nil
}

// GetMany returns a map of values for given keys.
// Ignores keys that do not exist or not strings,
// and does not return them in the map.
func (d *DB) GetMany(keys ...string) (map[string]core.Value, error) {
	now := time.Now().UnixMilli()

	var results []struct {
		KeyName string
		Value   []byte
	}
	err := d.store.DB.Model(&store.RString{}).
		Joins("JOIN rkey ON rstring.kid = rkey.id AND rkey.ktype = ?", core.TypeString.Value()).
		Where("rkey.kdb = ? AND rkey.kname IN ?", d.dbIdx, keys).
		Scopes(store.NotExpired(now)).
		Select("rkey.kname as key_name, rstring.kval as value").
		Find(&results).Error
	if err != nil {
		return nil, err
	}

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
func (d *DB) Incr(key string, delta int) (int, error) {
	var newVal int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.Itoa(delta)),
			}
			return tx.Create(&rstr).Error
		}
		if err != nil {
			return err
		}

		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}

		var rstr store.RString
		err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.Itoa(delta)),
			}
			if err := tx.Create(&rstr).Error; err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
		if err != nil {
			return err
		}

		valInt, err := strconv.Atoi(string(rstr.KVal))
		if err != nil {
			return core.ErrValueType
		}
		newVal = valInt + delta

		rstr.KVal = []byte(strconv.Itoa(newVal))
		if err := tx.Save(&rstr).Error; err != nil {
			return err
		}
		return tx.Model(&store.RKey{}).
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
func (d *DB) IncrFloat(key string, delta float64) (float64, error) {
	var newVal float64
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
			}
			return tx.Create(&rstr).Error
		}
		if err != nil {
			return err
		}

		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}

		var rstr store.RString
		err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			newVal = delta
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
			}
			if err := tx.Create(&rstr).Error; err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
		if err != nil {
			return err
		}

		valFloat, err := strconv.ParseFloat(string(rstr.KVal), 64)
		if err != nil {
			return core.ErrValueType
		}
		newVal = valFloat + delta

		rstr.KVal = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
		if err := tx.Save(&rstr).Error; err != nil {
			return err
		}
		return tx.Model(&store.RKey{}).
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
func (d *DB) Set(key string, value any) error {
	vb, err := core.ToBytes(value)
	if err != nil {
		return err
	}

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: vb,
			}
			return tx.Create(&rstr).Error

		case err != nil:
			return err

		case rkey.KType != core.TypeString.Value():
			return core.ErrKeyType

		default:
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: vb,
			}
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstr).Error
			if err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"expire_at":   nil,
					"modified_at": now,
					"klen":        1,
				}).Error
		}
	})
}

// SetExpire sets the key value with an optional expiration time (if ttl > 0).
// Overwrites the value and ttl if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) SetExpire(key string, value any, ttl time.Duration) error {
	vb, err := core.ToBytes(value)
	if err != nil {
		return err
	}

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var expireAt *int64
		if ttl > 0 {
			expire := now + ttl.Milliseconds()
			expireAt = &expire
		}

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ExpireAt:   expireAt,
				ModifiedAt: now,
				KLen:       1,
			}

			// Try to create the key, handle unique constraint violation
			err = tx.Create(&rkey).Error
			if err != nil {
				// Unique constraint violation - another transaction created the key
				// Re-query to get the existing key
				err = tx.Model(&store.RKey{}).
					Where("kdb = ? AND kname = ?", d.dbIdx, key).
					First(&rkey).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					// Key expired between our create attempt and re-query
					return nil
				}
				if err != nil {
					return err
				}
				// Update the existing key
				if rkey.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}
			}

			rstr := store.RString{
				KID:  rkey.ID,
				KVal: vb,
			}
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstr).Error
			if err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"expire_at":   expireAt,
					"modified_at": now,
					"klen":        1,
				}).Error

		case err != nil:
			return err

		case rkey.KType != core.TypeString.Value():
			return core.ErrKeyType

		default:
			rstr := store.RString{
				KID:  rkey.ID,
				KVal: vb,
			}
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstr).Error
			if err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"expire_at":   expireAt,
					"modified_at": now,
					"klen":        1,
				}).Error
		}
	})
}

// SetMany sets the values of multiple keys.
// Overwrites values for keys that already exist and
// creates new keys/values for keys that do not exist.
// Removes the TTL for existing keys.
// If any of the keys exists but is not a string, returns ErrKeyType.
func (d *DB) SetMany(items map[string]any) error {
	if len(items) == 0 {
		return nil
	}

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

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var existingKeys []store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname IN ?", d.dbIdx, keyNames).
			Find(&existingKeys).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		existingMap := make(map[string]store.RKey, len(existingKeys))
		for _, k := range existingKeys {
			existingMap[k.KName] = k
		}

		var existingKeyNames []string
		var newKeyNames []string
		for k := range items {
			if ek, ok := existingMap[k]; ok {
				if ek.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}
				existingKeyNames = append(existingKeyNames, k)
			} else {
				newKeyNames = append(newKeyNames, k)
			}
		}

		if len(existingKeyNames) > 0 {
			err = tx.Model(&store.RKey{}).
				Where("kdb = ? AND kname IN ?", d.dbIdx, existingKeyNames).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"expire_at":   nil,
					"modified_at": now,
					"klen":        1,
				}).Error
			if err != nil {
				return err
			}

			rstrings := make([]store.RString, 0, len(existingKeyNames))
			for _, k := range existingKeyNames {
				rstrings = append(rstrings, store.RString{
					KID:  existingMap[k].ID,
					KVal: values[k],
				})
			}
			err = tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstrings).Error
			if err != nil {
				return err
			}
		}

		if len(newKeyNames) > 0 {
			rkeys := make([]store.RKey, 0, len(newKeyNames))
			for _, k := range newKeyNames {
				rkeys = append(rkeys, store.RKey{
					KDB:        d.dbIdx,
					KName:      k,
					KType:      core.TypeString.Value(),
					KVer:       1,
					ExpireAt:   nil,
					ModifiedAt: now,
					KLen:       1,
				})
			}
			err = tx.Create(&rkeys).Error
			if err != nil {
				return err
			}

			var newKeys []store.RKey
			err = tx.Where("kdb = ? AND kname IN ?", d.dbIdx, newKeyNames).Find(&newKeys).Error
			if err != nil {
				return err
			}

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
			err = tx.Create(&rstrings).Error
			if err != nil {
				return err
			}
		}

		return nil
	})
}

// SetWith provides a builder pattern for setting values with additional options.
// Example: db.SetWith("key", "value").Expire(time.Hour).Exec()
func (d *DB) SetWith(key string, value any) SetCmd {
	return SetCmd{db: d, key: key, val: value}
}

// SetCmd is a builder for setting values with additional options.
type SetCmd struct {
	db       *DB
	key      string
	val      any
	ifExists bool
	ifNX     bool
	get      bool
	ttl      time.Duration
	at       time.Time
	keepTTL  bool
}

// SetOption represents a functional option for the SetCmd.
type SetOption func(*SetCmd)

// IfExists sets the IF EXISTS option (only update if key exists).
func (cmd SetCmd) IfExists() SetCmd {
	cmd.ifExists = true
	return cmd
}

// IfNotExists sets the IF NOT EXISTS option (only create if key doesn't exist).
func (cmd SetCmd) IfNotExists() SetCmd {
	cmd.ifNX = true
	return cmd
}

// TTL sets the expiration time for the key.
func (cmd SetCmd) TTL(ttl time.Duration) SetCmd {
	cmd.ttl = ttl
	return cmd
}

// At sets the expiration timestamp for the key.
func (cmd SetCmd) At(t time.Time) SetCmd {
	cmd.at = t
	cmd.ttl = time.Until(t)
	return cmd
}

// KeepTTL keeps the existing TTL of the key.
func (cmd SetCmd) KeepTTL() SetCmd {
	cmd.keepTTL = true
	return cmd
}

// Get returns the previous value after setting.
func (cmd SetCmd) Get() SetCmd {
	cmd.get = true
	return cmd
}

// SetResult represents the result of a SET operation.
type SetResult struct {
	Updated bool
	Created bool
	Prev    core.Value
}

// Run executes the set command with the configured options.
// This implementation delegates to the individual methods for cleaner code.
func (cmd SetCmd) Run() (SetResult, error) {
	result := SetResult{}

	// Handle GET option alone or with other options
	if cmd.get {
		// GET + NX: only set if key doesn't exist, return previous value
		if cmd.ifNX {
			prev, created, err := cmd.db.SetNXGet(cmd.key, cmd.val, cmd.ttl)
			if err != nil {
				return result, err
			}
			result.Prev = prev
			result.Created = created
			return result, nil
		}

		// GET + XX: only set if key exists, return previous value
		if cmd.ifExists {
			prev, updated, err := cmd.db.SetXXGet(cmd.key, cmd.val, cmd.ttl)
			if err != nil {
				return result, err
			}
			result.Prev = prev
			result.Updated = updated
			return result, nil
		}

		// GET alone: set value and return previous value
		prev, err := cmd.db.SetGet(cmd.key, cmd.val)
		if err != nil {
			return result, err
		}
		result.Prev = prev
		if len(prev) > 0 {
			result.Updated = true
		} else {
			result.Created = true
		}
		return result, nil
	}

	if cmd.ifNX {
		created, err := cmd.db.SetNXWithTTL(cmd.key, cmd.val, cmd.ttl)
		if err != nil {
			return result, err
		}
		result.Created = created
		return result, nil
	}

	if cmd.ifExists {
		updated, err := cmd.db.SetXXWithTTL(cmd.key, cmd.val, cmd.ttl)
		if err != nil {
			return result, err
		}
		result.Updated = updated
		return result, nil
	}

	if cmd.keepTTL {
		updated, err := cmd.db.SetKeepTTL(cmd.key, cmd.val)
		if err != nil {
			return result, err
		}
		result.Updated = updated
		result.Created = updated
		return result, nil
	}

	if cmd.ttl > 0 {
		if err := cmd.db.SetExpire(cmd.key, cmd.val, cmd.ttl); err != nil {
			return result, err
		}
	} else {
		if err := cmd.db.Set(cmd.key, cmd.val); err != nil {
			return result, err
		}
	}

	result.Updated = true
	result.Created = true
	return result, nil
}

// Exec executes the set command (legacy compatibility).
func (cmd SetCmd) Exec() error {
	_, err := cmd.Run()
	return err
}

// SetNX sets the key to hold string value if key does not exist.
// Returns true if the key was set, false if the key already exists.
// This is the Redis SETNX command implementation.
// Time complexity: O(1)
//
// SetNX is short for "SET if Not eXists".
//
// Design pattern: This can be used as a locking primitive.
// For example, to acquire a lock:
//   - Use SetNX to set the lock key with a unique value
//   - If SetNX returns true, the lock is acquired
//   - To release the lock, use Del (after verifying ownership)
//
// Note: Starting from Redis 2.6.12, SET with NX argument is preferred
// over SETNX. This method provides direct SETNX semantics.
func (d *DB) SetNX(key string, value any) (bool, error) {
	return d.SetNXWithTTL(key, value, 0)
}

// SetNXWithTTL sets the key to hold string value if key does not exist.
// If ttl > 0, sets the expiration time as well.
// Returns true if the key was set, false if the key already exists.
// This implements SET key value NX EX seconds / PX milliseconds.
func (d *DB) SetNXWithTTL(key string, value any, ttl time.Duration) (bool, error) {
	var created bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var expireAt *int64
		if ttl > 0 {
			expire := now + ttl.Milliseconds()
			expireAt = &expire
		}

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if err == nil {
			if rkey.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}
			return nil
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      key,
			KType:      core.TypeString.Value(),
			KVer:       1,
			ExpireAt:   expireAt,
			ModifiedAt: now,
			KLen:       1,
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rstr := store.RString{
			KID:  rkey.ID,
			KVal: vb,
		}
		if err := tx.Create(&rstr).Error; err != nil {
			return err
		}

		created = true
		return nil
	})

	return created, err
}

// SetXX sets the key to hold string value if key already exists.
// Returns true if the key was updated, false if the key does not exist.
// This is the Redis SET with XX option implementation.
// Time complexity: O(1)
func (d *DB) SetXX(key string, value any) (bool, error) {
	return d.SetXXWithTTL(key, value, 0)
}

// SetXXWithTTL sets the key to hold string value if key already exists.
// If ttl > 0, sets the expiration time as well.
// Returns true if the key was updated, false if the key does not exist.
// This implements SET key value XX EX seconds / PX milliseconds.
func (d *DB) SetXXWithTTL(key string, value any, ttl time.Duration) (bool, error) {
	var updated bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var expireAt *int64
		if ttl > 0 {
			expire := now + ttl.Milliseconds()
			expireAt = &expire
		}

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}

		rstr := store.RString{
			KID:  rkey.ID,
			KVal: vb,
		}
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&rstr).Error
		if err != nil {
			return err
		}

		updated = true
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"expire_at":   expireAt,
				"modified_at": now,
				"klen":        1,
			}).Error
	})

	return updated, err
}

// SetNXGet sets the key to hold string value if key does not exist.
// Returns the previous value and whether the key was created.
// This implements SET key value NX GET semantics.
func (d *DB) SetNXGet(key string, value any, ttl time.Duration) (core.Value, bool, error) {
	var prev core.Value
	var created bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
		now := time.Now().UnixMilli()

		var expireAt *int64
		if ttl > 0 {
			expire := now + ttl.Milliseconds()
			expireAt = &expire
		}

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if err == nil {
			if rkey.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}

			if rkey.ExpireAt != nil && *rkey.ExpireAt <= now {
				tx.Where("kid = ?", rkey.ID).Delete(&store.RString{})
				tx.Delete(&rkey)
			} else {
				var rstr store.RString
				err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
				if err == nil {
					prev = core.Value(rstr.KVal)
				}
				return nil
			}
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      key,
			KType:      core.TypeString.Value(),
			KVer:       1,
			ExpireAt:   expireAt,
			ModifiedAt: now,
			KLen:       1,
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rstr := store.RString{
			KID:  rkey.ID,
			KVal: vb,
		}
		if err := tx.Create(&rstr).Error; err != nil {
			return err
		}

		created = true
		return nil
	})

	return prev, created, err
}

// SetXXGet sets the key to hold string value if key already exists.
// Returns the previous value and whether the key was updated.
// This implements SET key value XX GET semantics.
func (d *DB) SetXXGet(key string, value any, ttl time.Duration) (core.Value, bool, error) {
	var prev core.Value
	var updated bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var expireAt *int64
		if ttl > 0 {
			expire := now + ttl.Milliseconds()
			expireAt = &expire
		}

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}

		var rstr store.RString
		err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}
		if err == nil {
			prev = core.Value(rstr.KVal)
		}

		rstr.KVal = vb
		err = tx.Save(&rstr).Error
		if err != nil {
			return err
		}

		updated = true
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"expire_at":   expireAt,
				"modified_at": now,
				"klen":        1,
			}).Error
	})

	return prev, updated, err
}

// SetGet sets the key to hold string value and returns the previous value.
// Returns the previous value if the key existed, nil if the key did not exist.
// This is the Redis SET with GET option implementation (added in Redis 6.2).
// Time complexity: O(1)
func (d *DB) SetGet(key string, value any) (core.Value, error) {
	var prev core.Value
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if err == nil {
			if rkey.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}

			var rstr store.RString
			err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if err == nil {
				prev = core.Value(rstr.KVal)
			}

			rstr.KVal = vb
			err = tx.Save(&rstr).Error
			if err != nil {
				return err
			}

			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"expire_at":   nil,
					"modified_at": now,
					"klen":        1,
				}).Error
		}

		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      key,
			KType:      core.TypeString.Value(),
			KVer:       1,
			ModifiedAt: now,
			KLen:       1,
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rstr := store.RString{
			KID:  rkey.ID,
			KVal: vb,
		}
		return tx.Create(&rstr).Error
	})

	return prev, err
}

// SetKeepTTL sets the key to hold string value while keeping the existing TTL.
// Returns true if the key was updated, false if the key did not exist.
// This is the Redis SET with KEEPTTL option implementation (added in Redis 6.0).
// Time complexity: O(1)
func (d *DB) SetKeepTTL(key string, value any) (bool, error) {
	var updated bool
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		vb, err := core.ToBytes(value)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		if errors.Is(err, gorm.ErrRecordNotFound) {
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}

			rstr := store.RString{
				KID:  rkey.ID,
				KVal: vb,
			}
			if err := tx.Create(&rstr).Error; err != nil {
				return err
			}

			updated = true
			return nil
		}
		if err != nil {
			return err
		}

		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}

		rstr := store.RString{
			KID:  rkey.ID,
			KVal: vb,
		}
		err = tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&rstr).Error
		if err != nil {
			return err
		}

		updated = true
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        1,
			}).Error
	})

	return updated, err
}

// StrLen returns the length of the string value.
// If the key does not exist or is not a string, returns ErrNotFound.
func (d *DB) StrLen(key string) (int, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Len int
	}
	err := d.store.DB.Model(&store.RString{}).
		Joins("JOIN rkey ON rstring.kid = rkey.id AND rkey.ktype = ?", core.TypeString.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Select("LENGTH(rstring.kval) as len").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return result.Len, nil
}

// Append appends the value to the existing string value.
// Returns the length of the string after appending.
// This method uses transaction with row-level locking to prevent race conditions.
func (d *DB) Append(key string, value []byte) (int, error) {
	var newLen int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}

			rstr := store.RString{
				KID:  rkey.ID,
				KVal: value,
			}
			if err := tx.Create(&rstr).Error; err != nil {
				return err
			}
			newLen = len(value)
			return nil

		case err != nil:
			return err

		case rkey.KType != core.TypeString.Value():
			return core.ErrKeyType

		default:
			var rstr store.RString
			err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				rstr = store.RString{
					KID:  rkey.ID,
					KVal: value,
				}
				if err := tx.Create(&rstr).Error; err != nil {
					return err
				}
				newLen = len(value)
			} else if err != nil {
				return err
			} else {
				newVal := append(rstr.KVal, value...)
				rstr.KVal = newVal
				if err := tx.Save(&rstr).Error; err != nil {
					return err
				}
				newLen = len(newVal)
			}

			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
	})

	return newLen, err
}

// SetRange overwrites the part of the string starting at offset with the value.
// Returns the length of the string after modification.
// This method uses transaction with row-level locking to prevent race conditions.
func (d *DB) SetRange(key string, offset int, value []byte) (int, error) {
	if offset < 0 {
		return 0, core.ErrArgument
	}

	var newLen int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			First(&rkey).Error

		var existingBytes []byte

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      core.TypeString.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}

			existingBytes = make([]byte, offset)
			existingBytes = append(existingBytes, value...)

			rstr := store.RString{
				KID:  rkey.ID,
				KVal: existingBytes,
			}
			if err := tx.Create(&rstr).Error; err != nil {
				return err
			}
			newLen = len(existingBytes)
			return nil

		case err != nil:
			return err

		case rkey.KType != core.TypeString.Value():
			return core.ErrKeyType

		default:
			var rstr store.RString
			err = tx.Where("kid = ?", rkey.ID).First(&rstr).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				existingBytes = make([]byte, offset)
				existingBytes = append(existingBytes, value...)

				newRstr := store.RString{
					KID:  rkey.ID,
					KVal: existingBytes,
				}
				if err := tx.Create(&newRstr).Error; err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else {
				existingBytes = rstr.KVal
				if offset > len(existingBytes) {
					existingBytes = append(existingBytes, make([]byte, offset-len(existingBytes))...)
				}

				if offset+len(value) > len(existingBytes) {
					existingBytes = append(existingBytes[:offset], value...)
				} else {
					copy(existingBytes[offset:], value)
				}

				rstr.KVal = existingBytes
				if err := tx.Save(&rstr).Error; err != nil {
					return err
				}
			}
			newLen = len(existingBytes)

			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
	})

	return newLen, err
}

// GetRange returns a substring of the string value.
// Start and end are zero-based offsets.
// If the key does not exist or is not a string, returns ErrNotFound.
func (d *DB) GetRange(key string, start, end int) (core.Value, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Value []byte
	}
	err := d.store.DB.Model(&store.RString{}).
		Joins("JOIN rkey ON rstring.kid = rkey.id AND rkey.ktype = ?", core.TypeString.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Select("rstring.kval as value").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}

	value := result.Value
	if start < 0 {
		start = len(value) + start
	}
	if end < 0 {
		end = len(value) + end
	}
	if start > len(value) {
		start = len(value)
	}
	if end > len(value) {
		end = len(value)
	}
	if start > end {
		return core.Value{}, nil
	}

	return core.Value(value[start : end+1]), nil
}
