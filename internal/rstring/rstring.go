// Package rstring is a database-backed string repository.
// It provides methods to interact with strings in the database.
package rstring

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"strings"
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

	var row struct {
		KType    int    `gorm:"column:ktype"`
		ExpireAt *int64 `gorm:"column:expire_at"`
		KVal     []byte `gorm:"column:kval"`
	}
	err := d.store.DB.Model(&store.RKey{}).
		Select("rkey.ktype, rkey.expire_at, rstring.kval").
		Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		First(&row).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}

	// Lazy expiration: check if key is expired
	if row.ExpireAt != nil && *row.ExpireAt <= now {
		return core.Value{}, core.ErrNotFound
	}

	if row.KType != core.TypeString.Value() {
		return core.Value{}, core.ErrKeyType
	}
	if row.KVal == nil {
		return core.Value{}, core.ErrNotFound
	}
	return core.Value(row.KVal), nil
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
	value := 0
	item := incrWriteItem{delta: delta, value: &value}

	err := d.store.TransactionWriteItems(context.Background(),
		"string:incr:"+strconv.Itoa(d.dbIdx)+"\x00"+key, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				incs := make([]incrWriteItem, 0, len(items))
				total := 0
				for _, raw := range items {
					inc := raw.(incrWriteItem)
					incs = append(incs, inc)
					total += inc.delta
				}

				now := time.Now().UnixMilli()
				var current struct {
					ID    int    `gorm:"column:id"`
					KType int    `gorm:"column:ktype"`
					KVal  []byte `gorm:"column:kval"`
				}
				currentQuery := tx.Model(&store.RKey{}).
					Select("rkey.id, rkey.ktype, rstring.kval").
					Joins("JOIN rstring ON rstring.kid = rkey.id").
					Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
				if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
					currentQuery = currentQuery.Clauses(clause.Locking{Strength: "UPDATE"})
				}
				err := currentQuery.First(&current).Error

				var final int
				if err == nil {
					if current.KType != core.TypeString.Value() {
						return core.ErrKeyType
					}
					start, convErr := strconv.Atoi(string(current.KVal))
					if convErr != nil {
						return core.ErrValueType
					}
					final = d.assignIncrResults(incs, start)
					if final == start {
						return nil
					}
					return tx.Model(&store.RString{}).
						Where("kid = ?", current.ID).
						Update("kval", []byte(strconv.Itoa(final))).Error
				}
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}

				rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
					core.TypeString.Value(), now, 1)
				if err != nil {
					return err
				}
				if rkey.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}

				var rstr store.RString
				query := tx.Where("kid = ?", rkey.ID)
				if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
					query = query.Clauses(clause.Locking{Strength: "UPDATE"})
				}
				err = query.First(&rstr).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					final = d.assignIncrResults(incs, 0)
					rstr := store.RString{
						KID:  rkey.ID,
						KVal: []byte(strconv.Itoa(final)),
					}
					return tx.Create(&rstr).Error
				}
				if err != nil {
					return err
				}
				start, convErr := strconv.Atoi(string(rstr.KVal))
				if convErr != nil {
					return core.ErrValueType
				}
				final = d.assignIncrResults(incs, start)
				rstr.KVal = []byte(strconv.Itoa(final))
				return tx.Save(&rstr).Error
			}
			if len(items) == 1 && !d.store.InTx() {
				return tx.Transaction(func(tx *gorm.DB) error {
					return run(tx)
				})
			}
			return run(tx)
		})

	return value, err
}

func (d *DB) assignIncrResults(items []incrWriteItem, start int) int {
	current := start
	for _, item := range items {
		current += item.delta
		*item.value = current
	}
	return current
}

type incrWriteItem struct {
	delta int
	value *int
}

// IncrFloat increments the float key value by the specified amount.
// Returns the value after the increment.
// If the key does not exist, sets it to 0 before the increment.
// If the key value is not an float, returns ErrValueType.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) IncrFloat(key string, delta float64) (float64, error) {
	newValue := 0.0
	item := incrFloatWriteItem{delta: delta, value: &newValue}
	err := d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			incs := make([]incrFloatWriteItem, 0, len(items))
			for _, raw := range items {
				inc := raw.(incrFloatWriteItem)
				incs = append(incs, inc)
			}

			var current struct {
				ID       int    `gorm:"column:id"`
				KType    int    `gorm:"column:ktype"`
				KVal     []byte `gorm:"column:kval"`
				HasValue bool   `gorm:"column:has_value"`
			}
			query := tx.Model(&store.RKey{}).
				Select("rkey.id, rkey.ktype, rstring.kval, rstring.kid IS NOT NULL AS has_value").
				Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
				Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
			// PostgreSQL cannot lock the nullable side of this outer join.
			if dialect == store.DialectMySQL {
				query = query.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			err := query.First(&current).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				rkey, err := store.EnsureKeyLocked(tx, dialect, d.dbIdx, key,
					core.TypeString.Value(), now, 1)
				if err != nil {
					return err
				}
				current.ID = rkey.ID
				current.KType = rkey.KType
			} else if err != nil {
				return err
			}
			if current.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}

			start := 0.0
			if current.HasValue {
				start, err = strconv.ParseFloat(string(current.KVal), 64)
				if err != nil {
					return core.ErrValueType
				}
			}
			final := d.assignIncrFloatResults(incs, start)
			value := []byte(strconv.FormatFloat(final, 'f', -1, 64))
			if current.HasValue {
				if dialect == store.DialectMySQL {
					return tx.Exec(
						"UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid "+
							"SET s.kval = ?, k.kver = k.kver + ?, k.modified_at = ? "+
							"WHERE s.kid = ?",
						value, len(items), now, current.ID,
					).Error
				}
				if err := tx.Model(&store.RString{}).
					Where("kid = ?", current.ID).
					Update("kval", value).Error; err != nil {
					return err
				}
			} else {
				row := store.RString{KID: current.ID, KVal: value}
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", current.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + ?", len(items)),
					"modified_at": now,
				}).Error
		})

	return newValue, err
}

func (d *DB) assignIncrFloatResults(items []incrFloatWriteItem, start float64) float64 {
	current := start
	for _, item := range items {
		current += item.delta
		*item.value = current
	}
	return current
}

type incrFloatWriteItem struct {
	delta float64
	value *float64
}

// Set sets the key value that will not expire.
// Overwrites the value if the key already exists.
// If the key exists but is not a string, returns ErrKeyType.
func (d *DB) Set(key string, value any) error {
	vb, err := core.ToBytes(value)
	if err != nil {
		return err
	}

	item := setWriteItem{value: vb}
	return d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			last := items[len(items)-1].(setWriteItem)
			if last.expireAt == nil {
				return d.setNonBatched(tx, key, last.value)
			}
			return d.setExpireNonBatched(tx, key, last.value,
				time.Duration(*last.expireAt-time.Now().UnixMilli())*time.Millisecond)
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
	if ttl <= 0 {
		return d.Set(key, value)
	}
	expireAt := time.Now().UnixMilli() + ttl.Milliseconds()
	item := setWriteItem{value: vb, expireAt: &expireAt}
	return d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			last := items[len(items)-1].(setWriteItem)
			if last.expireAt == nil {
				return d.setNonBatched(tx, key, last.value)
			}
			return d.setExpireNonBatched(tx, key, last.value,
				time.Duration(*last.expireAt-time.Now().UnixMilli())*time.Millisecond)
		})
}

type setWriteItem struct {
	value    []byte
	expireAt *int64
}

func (d *DB) setNonBatched(db *gorm.DB, key string, vb []byte) error {
	if d.store.Dialect == store.DialectMySQL {
		var result *gorm.DB
		result = db.Exec(`
			UPDATE rstring AS s
			JOIN rkey AS k ON k.id = s.kid
			SET s.kval = ?
			WHERE k.kdb = ? AND k.kname = ?
				AND k.ktype = ? AND k.expire_at IS NULL`,
			vb, d.dbIdx, key, core.TypeString.Value())
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			return nil
		}
	}

	var current struct {
		ID       int    `gorm:"column:id"`
		KType    int    `gorm:"column:ktype"`
		ExpireAt *int64 `gorm:"column:expire_at"`
		KVal     []byte `gorm:"column:kval"`
	}
	err := db.Model(&store.RKey{}).
		Select("rkey.id, rkey.ktype, rkey.expire_at, rstring.kval").
		Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		First(&current).Error
	if err == nil && d.store.Dialect != store.DialectMySQL &&
		current.KType == core.TypeString.Value() &&
		current.ExpireAt == nil && bytes.Equal(current.KVal, vb) {
		return nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	return db.Transaction(func(tx *gorm.DB) error {
		dialect := d.store.Dialect
		now := time.Now().UnixMilli()
		var current struct {
			ID       int    `gorm:"column:id"`
			KType    int    `gorm:"column:ktype"`
			ExpireAt *int64 `gorm:"column:expire_at"`
			KVal     []byte `gorm:"column:kval"`
		}
		err := tx.Model(&store.RKey{}).
			Select("rkey.id, rkey.ktype, rkey.expire_at, rstring.kval").
			Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
			Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
			First(&current).Error
		if err == nil {
			if current.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}
			if current.KVal != nil && bytes.Equal(current.KVal, vb) && current.ExpireAt == nil {
				return nil
			}
			rstr := store.RString{KID: current.ID, KVal: vb}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstr).Error; err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", current.ID).
				Updates(map[string]interface{}{
					"expire_at":   nil,
					"modified_at": now,
					"klen":        1,
				}).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
			core.TypeString.Value(), now, 1)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
		rstr := store.RString{KID: rkey.ID, KVal: vb}
		result := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&rstr)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 && rkey.ExpireAt == nil {
			return nil
		}
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"expire_at":   nil,
				"modified_at": now,
				"klen":        1,
			}).Error
	})
}

func (d *DB) writeScope(key string) string {
	return "string:write:" + strconv.Itoa(d.dbIdx) + "\x00" + key
}

func (d *DB) setExpireNonBatched(db *gorm.DB, key string, vb []byte, ttl time.Duration) error {
	now := time.Now().UnixMilli()
	expire := now + ttl.Milliseconds()
	if d.store.Dialect == store.DialectMySQL {
		result := db.Exec(`
			UPDATE rkey AS k
			JOIN rstring AS s ON s.kid = k.id
			SET s.kval = ?, k.expire_at = ?, k.modified_at = ?
			WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ?`,
			vb, expire, now, d.dbIdx, key, core.TypeString.Value())
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected > 0 {
			return nil
		}
	}

	return db.Transaction(func(tx *gorm.DB) error {
		dialect := d.store.Dialect
		var current struct {
			ID       int    `gorm:"column:id"`
			KType    int    `gorm:"column:ktype"`
			ExpireAt *int64 `gorm:"column:expire_at"`
			KVal     []byte `gorm:"column:kval"`
		}
		err := tx.Model(&store.RKey{}).
			Select("rkey.id, rkey.ktype, rkey.expire_at, rstring.kval").
			Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
			Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
			First(&current).Error
		if err == nil {
			if current.KType != core.TypeString.Value() {
				return core.ErrKeyType
			}
			if current.KVal != nil && bytes.Equal(current.KVal, vb) &&
				current.ExpireAt != nil && *current.ExpireAt == expire {
				return nil
			}
			rstr := store.RString{KID: current.ID, KVal: vb}
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "kid"}},
				DoUpdates: clause.AssignmentColumns([]string{"kval"}),
			}).Create(&rstr).Error; err != nil {
				return err
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", current.ID).
				Updates(map[string]interface{}{
					"expire_at":   expire,
					"modified_at": now,
					"klen":        1,
				}).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
			core.TypeString.Value(), now, 1)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
		rstr := store.RString{KID: rkey.ID, KVal: vb}
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}},
			DoUpdates: clause.AssignmentColumns([]string{"kval"}),
		}).Create(&rstr).Error; err != nil {
			return err
		}
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"expire_at":   expire,
				"modified_at": now,
				"klen":        1,
			}).Error
	})
}

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

	sort.Strings(keyNames)
	item := setManyWriteItem{values: values}
	scope := "string:mset:" + strconv.Itoa(d.dbIdx) + "\x00"
	for _, name := range keyNames {
		// Length prefixes keep the routing scope unambiguous even when key
		// names contain separator bytes.
		scope += strconv.Itoa(len(name)) + ":" + name + "\x1f"
	}

	return d.store.TransactionWriteItems(context.Background(), scope, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			final := make(map[string][]byte, len(values))
			for _, raw := range items {
				for name, value := range raw.(setManyWriteItem).values {
					final[name] = value
				}
			}

			if dialect == store.DialectMySQL {
				if err := d.setManyBatchMySQL(tx, keyNames, final); err == nil {
					return nil
				} else if !errors.Is(err, core.ErrKeyType) {
					return err
				}
			} else if dialect == store.DialectPostgres {
				if err := d.setManyBatchPostgres(tx, keyNames, final); err == nil {
					return nil
				} else if !errors.Is(err, core.ErrKeyType) {
					return err
				}
			} else if updated, err := d.updateSetManyFast(tx, keyNames, final); err != nil || updated {
				return err
			}
			run := func(tx *gorm.DB) error {
				return d.setManyTransactional(tx, dialect, keyNames, final)
			}
			if d.store.InTx() || len(items) > 1 {
				return run(tx)
			}
			return tx.Transaction(run)
		})
}

type setManyWriteItem struct {
	values map[string][]byte
}

func (d *DB) setManyTransactional(tx *gorm.DB, dialect store.Dialect,
	keyNames []string, values map[string][]byte) error {
	now := time.Now().UnixMilli()
	keys := make([]store.RKey, 0, len(keyNames))
	rows := make([]store.RString, 0, len(keyNames))
	for _, name := range keyNames {
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, name,
			core.TypeString.Value(), now, 1)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
		keys = append(keys, rkey)
		rows = append(rows, store.RString{KID: rkey.ID, KVal: values[name]})
	}

	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kid"}},
		DoUpdates: clause.AssignmentColumns([]string{"kval"}),
	}).Create(&rows).Error; err != nil {
		return err
	}

	for _, rkey := range keys {
		if rkey.ExpireAt == nil && rkey.KLen == 1 {
			continue
		}
		if err := tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"expire_at":   nil,
				"modified_at": now,
				"klen":        1,
			}).Error; err != nil {
			return err
		}
	}
	return nil
}

// updateSetManyFast replaces values already known to be plain, existing string
// keys in one statement. The count guard makes the statement all-or-nothing;
// when any key is missing, expired, or has non-default metadata, callers use
// the regular transactional path.
func (d *DB) updateSetManyFast(db *gorm.DB, keyNames []string,
	values map[string][]byte) (bool, error) {
	if len(keyNames) == 0 {
		return true, nil
	}

	var sql strings.Builder
	var args []any
	switch d.store.Dialect {
	case store.DialectMySQL:
		sql.WriteString("UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid SET s.kval = CASE k.kname")
	case store.DialectPostgres:
		sql.WriteString("UPDATE rstring AS s SET kval = CASE k.kname")
	default:
		// SQLite has no portable UPDATE...FROM shape here. Its small local
		// writes are better served by the transactional implementation.
		return false, nil
	}

	for _, name := range keyNames {
		sql.WriteString(" WHEN ? THEN ?")
		args = append(args, name, values[name])
	}
	sql.WriteString(" ELSE s.kval END")

	var wherePrefix string
	switch d.store.Dialect {
	case store.DialectMySQL:
		wherePrefix = " WHERE k.kdb = ? AND k.kname IN ? AND k.ktype = ? AND k.expire_at IS NULL AND k.klen = 1"
	case store.DialectPostgres:
		wherePrefix = " FROM rkey AS k WHERE k.id = s.kid AND k.kdb = ? AND k.kname IN ? AND k.ktype = ? AND k.expire_at IS NULL AND k.klen = 1"
	default:
		return false, nil
	}
	sql.WriteString(wherePrefix)
	args = append(args, d.dbIdx, keyNames, core.TypeString.Value())

	sql.WriteString(" AND (SELECT COUNT(*) FROM rkey AS k2 JOIN rstring AS s2 ON s2.kid = k2.id WHERE k2.kdb = ? AND k2.kname IN ? AND k2.ktype = ? AND k2.expire_at IS NULL AND k2.klen = ?) = ?")
	args = append(args, d.dbIdx, keyNames, core.TypeString.Value(), 1, len(keyNames))

	result := db.Exec(sql.String(), args...)
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected == int64(len(keyNames)), nil
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
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
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

		rkey, inserted, err := store.InsertKeyIfAbsent(tx, dialect, d.dbIdx, key,
			core.TypeString.Value(), now, 1)
		if err != nil {
			return err
		}
		if inserted {
			if err := tx.Create(&store.RString{KID: rkey.ID, KVal: vb}).Error; err != nil {
				return err
			}
			created = true
			if expireAt == nil {
				return nil
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Update("expire_at", expireAt).Error
		}
		if rkey.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
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
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
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
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
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
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
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
	vb, err := core.ToBytes(value)
	if err != nil {
		return nil, err
	}

	prev := core.Value(nil)
	item := setGetWriteItem{value: vb, prev: &prev}
	err = d.store.TransactionWriteItems(context.Background(), d.writeScope(key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				var current struct {
					ID       int    `gorm:"column:id"`
					KType    int    `gorm:"column:ktype"`
					KVal     []byte `gorm:"column:kval"`
					HasValue bool   `gorm:"column:has_value"`
				}
				query := tx.Model(&store.RKey{}).
					Select("rkey.id, rkey.ktype, rstring.kval, rstring.kid IS NOT NULL AS has_value").
					Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
					Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
				err := query.First(&current).Error
				if err == nil {
					if current.KType != core.TypeString.Value() {
						return core.ErrKeyType
					}

					currentValue := core.Value(current.KVal)
					var final []byte
					for _, raw := range items {
						setGet := raw.(setGetWriteItem)
						*setGet.prev = currentValue
						currentValue = core.Value(setGet.value)
						final = setGet.value
					}
					if current.HasValue && dialect == store.DialectMySQL {
						return tx.Exec(
							"UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid "+
								"SET s.kval = ?, k.kver = k.kver + ?, k.expire_at = NULL, "+
								"k.modified_at = ?, k.klen = 1 WHERE s.kid = ?",
							final, len(items), now, current.ID,
						).Error
					}

					if current.HasValue {
						if err := tx.Model(&store.RString{}).
							Where("kid = ?", current.ID).
							Update("kval", final).Error; err != nil {
							return err
						}
					} else {
						row := store.RString{KID: current.ID, KVal: final}
						if err := tx.Create(&row).Error; err != nil {
							return err
						}
					}
					return tx.Model(&store.RKey{}).
						Where("id = ?", current.ID).
						Updates(map[string]interface{}{
							"kver":        gorm.Expr("kver + ?", len(items)),
							"expire_at":   nil,
							"modified_at": now,
							"klen":        1,
						}).Error
				}
				if !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}

				rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
					core.TypeString.Value(), now, 1)
				if err != nil {
					return err
				}
				if rkey.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}

				currentValue := core.Value(nil)
				var final []byte
				for _, raw := range items {
					setGet := raw.(setGetWriteItem)
					*setGet.prev = currentValue
					currentValue = core.Value(setGet.value)
					final = setGet.value
				}
				row := store.RString{KID: rkey.ID, KVal: final}
				if err := tx.Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "kid"}},
					DoUpdates: clause.AssignmentColumns([]string{"kval"}),
				}).Create(&row).Error; err != nil {
					return err
				}
				return tx.Model(&store.RKey{}).
					Where("id = ?", rkey.ID).
					Updates(map[string]interface{}{
						"kver":        gorm.Expr("kver + ?", len(items)),
						"expire_at":   nil,
						"modified_at": now,
						"klen":        1,
					}).Error
			}
			if len(items) == 1 && !d.store.InTx() && dialect != store.DialectMySQL {
				return tx.Transaction(func(tx *gorm.DB) error {
					return run(tx)
				})
			}
			return run(tx)
		})
	return prev, err
}

type setGetWriteItem struct {
	value []byte
	prev  *core.Value
}

// SetKeepTTL sets the key to hold string value while keeping the existing TTL.
// Returns true if the key was updated, false if the key did not exist.
// This is the Redis SET with KEEPTTL option implementation (added in Redis 6.0).
// Time complexity: O(1)
func (d *DB) SetKeepTTL(key string, value any) (bool, error) {
	var updated bool
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
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
func (d *DB) Append(key string, value []byte) (int, error) {
	newLen := 0
	item := appendWriteItem{value: value, length: &newLen}
	err := d.store.TransactionWriteItems(context.Background(), d.writeScope(key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				var current struct {
					ID       int    `gorm:"column:id"`
					KType    int    `gorm:"column:ktype"`
					ExpireAt *int64 `gorm:"column:expire_at"`
					KVal     []byte `gorm:"column:kval"`
					Len      int    `gorm:"column:len"`
					HasValue bool   `gorm:"column:has_value"`
				}

				combined := make([]byte, 0)
				for _, raw := range items {
					combined = append(combined, raw.(appendWriteItem).value...)
				}

				if dialect == store.DialectMySQL {
					result := tx.Exec(
						"UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid "+
							"SET s.kval = CONCAT(s.kval, ?), k.kver = k.kver + ?, "+
							"k.modified_at = ? WHERE k.kdb = ? AND k.kname = ? "+
							"AND k.ktype = ? AND (k.expire_at IS NULL OR k.expire_at > ?)",
						combined, len(items), now, d.dbIdx, key,
						core.TypeString.Value(), now,
					)
					if result.Error != nil {
						return result.Error
					}
					if result.RowsAffected > 0 {
						var final struct {
							Len int `gorm:"column:len"`
						}
						if err := tx.Raw(
							"SELECT LENGTH(s.kval) AS len FROM rstring AS s "+
								"JOIN rkey AS k ON k.id = s.kid "+
								"WHERE k.kdb = ? AND k.kname = ?",
							d.dbIdx, key,
						).Scan(&final).Error; err != nil {
							return err
						}
						lengths := make([]int, len(items))
						later := 0
						for i := len(items) - 1; i >= 0; i-- {
							lengths[i] = final.Len - later
							later += len(items[i].(appendWriteItem).value)
						}
						for i, raw := range items {
							*raw.(appendWriteItem).length = lengths[i]
						}
						return nil
					}
				}

				selectColumns := "rkey.id, rkey.ktype, rkey.expire_at, rstring.kval, rstring.kid IS NOT NULL AS has_value"
				if dialect == store.DialectMySQL {
					selectColumns = "rkey.id, rkey.ktype, rkey.expire_at, LENGTH(rstring.kval) AS len, rstring.kid IS NOT NULL AS has_value"
				}
				query := tx.Model(&store.RKey{}).
					Select(selectColumns).
					Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
					Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
				// PostgreSQL cannot lock the nullable side of this outer join.
				if dialect == store.DialectMySQL {
					query = query.Clauses(clause.Locking{Strength: "UPDATE"})
				}
				err := query.First(&current).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					rkey, err := store.EnsureKeyLocked(tx, dialect, d.dbIdx, key,
						core.TypeString.Value(), now, 1)
					if err != nil {
						return err
					}
					current.ID = rkey.ID
					current.KType = rkey.KType
				} else if err != nil {
					return err
				}
				if current.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}
				expired := current.ExpireAt != nil && *current.ExpireAt <= now
				if expired {
					current.HasValue = false
					current.KVal = nil
					current.Len = 0
				}

				currentLen := current.Len
				if dialect != store.DialectMySQL {
					currentLen = len(current.KVal)
				}
				for _, raw := range items {
					appendItem := raw.(appendWriteItem)
					currentLen += len(appendItem.value)
					*appendItem.length = currentLen
				}

				if current.HasValue && dialect == store.DialectMySQL {
					return tx.Exec(
						"UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid "+
							"SET s.kval = CONCAT(s.kval, ?), k.kver = k.kver + ?, k.modified_at = ? "+
							"WHERE s.kid = ?",
						combined, len(items), now, current.ID,
					).Error
				}

				value := current.KVal
				value = append(value, combined...)
				if value == nil {
					value = []byte{}
				}
				row := store.RString{KID: current.ID, KVal: value}
				if current.HasValue {
					if err := tx.Model(&store.RString{}).
						Where("kid = ?", current.ID).
						Update("kval", value).Error; err != nil {
						return err
					}
				} else {
					if expired {
						if err := tx.Where("kid = ?", current.ID).
							Delete(&store.RString{}).Error; err != nil {
							return err
						}
					}
					if err := tx.Create(&row).Error; err != nil {
						return err
					}
				}
				updates := map[string]interface{}{
					"kver":        gorm.Expr("kver + ?", len(items)),
					"modified_at": now,
				}
				if expired {
					updates["expire_at"] = nil
					updates["klen"] = 1
				}
				return tx.Model(&store.RKey{}).
					Where("id = ?", current.ID).
					Updates(updates).Error
			}
			return run(tx)
		})
	return newLen, err
}

type appendWriteItem struct {
	value  []byte
	length *int
}

// SetRange overwrites the part of the string starting at offset with the value.
// Returns the length of the string after modification.
// This method uses transaction with row-level locking to prevent race conditions.
func (d *DB) SetRange(key string, offset int, value []byte) (int, error) {
	if offset < 0 {
		return 0, core.ErrArgument
	}

	newLen := 0
	item := setRangeWriteItem{offset: offset, value: value, length: &newLen}
	err := d.store.TransactionWriteItems(context.Background(), d.writeScope(key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				var current struct {
					ID       int    `gorm:"column:id"`
					KType    int    `gorm:"column:ktype"`
					KVal     []byte `gorm:"column:kval"`
					HasValue bool   `gorm:"column:has_value"`
				}
				query := tx.Model(&store.RKey{}).
					Select("rkey.id, rkey.ktype, rstring.kval, rstring.kid IS NOT NULL AS has_value").
					Joins("LEFT JOIN rstring ON rstring.kid = rkey.id").
					Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
				// PostgreSQL cannot lock the nullable side of this outer join.
				if dialect == store.DialectMySQL {
					query = query.Clauses(clause.Locking{Strength: "UPDATE"})
				}
				err := query.First(&current).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					rkey, err := store.EnsureKeyLocked(tx, dialect, d.dbIdx, key,
						core.TypeString.Value(), now, 1)
					if err != nil {
						return err
					}
					current.ID = rkey.ID
					current.KType = rkey.KType
				} else if err != nil {
					return err
				}
				if current.KType != core.TypeString.Value() {
					return core.ErrKeyType
				}

				value := current.KVal
				for _, raw := range items {
					rangeItem := raw.(setRangeWriteItem)
					if rangeItem.offset > len(value) {
						value = append(value, make([]byte, rangeItem.offset-len(value))...)
					}
					end := rangeItem.offset + len(rangeItem.value)
					if end > len(value) {
						value = append(value, make([]byte, end-len(value))...)
					}
					copy(value[rangeItem.offset:end], rangeItem.value)
					*rangeItem.length = len(value)
				}

				if current.HasValue && dialect == store.DialectMySQL {
					return tx.Exec(
						"UPDATE rstring AS s JOIN rkey AS k ON k.id = s.kid "+
							"SET s.kval = ?, k.kver = k.kver + ?, k.expire_at = NULL, "+
							"k.modified_at = ?, k.klen = 1 WHERE s.kid = ?",
						value, len(items), now, current.ID,
					).Error
				}

				row := store.RString{KID: current.ID, KVal: value}
				if current.HasValue {
					if err := tx.Model(&store.RString{}).
						Where("kid = ?", current.ID).
						Update("kval", value).Error; err != nil {
						return err
					}
				} else if err := tx.Create(&row).Error; err != nil {
					return err
				}
				return tx.Model(&store.RKey{}).
					Where("id = ?", current.ID).
					Updates(map[string]interface{}{
						"kver":        gorm.Expr("kver + ?", len(items)),
						"expire_at":   nil,
						"modified_at": now,
						"klen":        1,
					}).Error
			}
			return run(tx)
		})
	return newLen, err
}

type setRangeWriteItem struct {
	offset int
	value  []byte
	length *int
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

// setManyBatchMySQL replaces all keys in one or two statements: an
// INSERT...ON DUPLICATE KEY UPDATE into rkey (with the string type already
// known) plus a multi-row INSERT...ON DUPLICATE KEY UPDATE into rstring. The
// rkey insert can fail for non-string keys with a duplicate-key error because
// the (kdb, kname) unique key collides on ktype, so callers detect that and
// fall back to the transactional path.
func (d *DB) setManyBatchMySQL(db *gorm.DB, keyNames []string, values map[string][]byte) error {
	if len(keyNames) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()

	var sb strings.Builder
	sb.WriteString("INSERT INTO rkey (kdb, kname, ktype, kver, modified_at, klen, klist_head, klist_tail) VALUES ")
	keyArgs := make([]any, 0, len(keyNames)*6)
	for i, name := range keyNames {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString("(?, ?, ?, 1, ?, 1, NULL, NULL)")
		keyArgs = append(keyArgs, d.dbIdx, name, core.TypeString.Value(), now)
	}
	sb.WriteString(" ON DUPLICATE KEY UPDATE modified_at = VALUES(modified_at), klen = VALUES(klen), expire_at = NULL")

	if err := db.Exec(sb.String(), keyArgs...).Error; err != nil {
		return err
	}

	var rows []struct {
		ID    int    `gorm:"column:id"`
		KName string `gorm:"column:kname"`
		KType int    `gorm:"column:ktype"`
	}
	if err := db.Model(&store.RKey{}).
		Select("id, kname, ktype").
		Where("kdb = ? AND kname IN ?", d.dbIdx, keyNames).
		Find(&rows).Error; err != nil {
		return err
	}

	stringIDs := make([]any, 0, len(rows))
	rstringRows := make([]store.RString, 0, len(rows))
	for _, row := range rows {
		if row.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
		stringIDs = append(stringIDs, row.ID)
		rstringRows = append(rstringRows, store.RString{
			KID:  row.ID,
			KVal: values[row.KName],
		})
	}

	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kid"}},
		DoUpdates: clause.AssignmentColumns([]string{"kval"}),
	}).Create(&rstringRows).Error; err != nil {
		return err
	}

	return db.Model(&store.RKey{}).
		Where("id IN ?", stringIDs).
		Where("expire_at IS NOT NULL OR kver > 1 OR klen != 1").
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"expire_at":   nil,
			"modified_at": now,
			"klen":        1,
		}).Error
}

func (d *DB) setManyBatchPostgres(db *gorm.DB, keyNames []string, values map[string][]byte) error {
	if len(keyNames) == 0 {
		return nil
	}
	now := time.Now().UnixMilli()

	rkeyRows := make([]store.RKey, 0, len(keyNames))
	for _, name := range keyNames {
		rkeyRows = append(rkeyRows, store.RKey{
			KDB:        d.dbIdx,
			KName:      name,
			KType:      core.TypeString.Value(),
			KVer:       1,
			ModifiedAt: now,
			KLen:       1,
		})
	}
	if err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "kdb"}, {Name: "kname"}},
		DoUpdates: clause.Assignments(map[string]any{
			"modified_at": gorm.Expr("EXCLUDED.modified_at"),
			"klen":        gorm.Expr("EXCLUDED.klen"),
			"expire_at":   nil,
		}),
	}).Create(&rkeyRows).Error; err != nil {
		return err
	}

	var rows []struct {
		ID    int    `gorm:"column:id"`
		KType int    `gorm:"column:ktype"`
		KName string `gorm:"column:kname"`
	}
	if err := db.Model(&store.RKey{}).
		Select("id, kname, ktype").
		Where("kdb = ? AND kname IN ?", d.dbIdx, keyNames).
		Find(&rows).Error; err != nil {
		return err
	}

	rstringRows := make([]store.RString, 0, len(rows))
	stringIDs := make([]any, 0, len(rows))
	for _, row := range rows {
		if row.KType != core.TypeString.Value() {
			return core.ErrKeyType
		}
		stringIDs = append(stringIDs, row.ID)
		rstringRows = append(rstringRows, store.RString{
			KID:  row.ID,
			KVal: values[row.KName],
		})
	}

	if err := db.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "kid"}},
		DoUpdates: clause.AssignmentColumns([]string{"kval"}),
	}).Create(&rstringRows).Error; err != nil {
		return err
	}

	return db.Model(&store.RKey{}).
		Where("id IN ?", stringIDs).
		Where("expire_at IS NOT NULL OR kver > 1 OR klen != 1").
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"expire_at":   nil,
			"modified_at": now,
			"klen":        1,
		}).Error
}
