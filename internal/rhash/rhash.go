// Package rhash is a database-backed hash repository.
// It provides methods to interact with hashmaps in the database.
package rhash

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"strconv"
	"strings"
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
//
// This is a simplified architecture that directly uses store.Store
// without additional transaction wrappers. Each method handles
// its own transactions internally when needed.
type DB struct {
	store *store.Store
	dbIdx int
}

// Scanner is the iterator for hash items.
// Stops when there are no more items or an error occurs.
type Scanner struct {
	db       *DB
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
	return &DB{store: s, dbIdx: 0}
}

// WithDB changes the logical database index in place and returns the same DB.
// It is safe for concurrent use; each TCP connection has its own DB instance.
func (d *DB) WithDB(dbIdx int) *DB {
	newDB := *d
	newDB.dbIdx = dbIdx
	return &newDB
}

// Delete deletes one or more items from a hash.
// Returns the number of fields deleted.
// Ignores non-existing fields.
// Does nothing if the key does not exist or is not a hash.
// This method uses transaction with row-level locking to prevent race conditions.
func (d *DB) Delete(key string, fields ...string) (int, error) {
	if len(fields) == 0 {
		return 0, core.ErrArgument
	}

	var n int64
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id").
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeHash.Value()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		result := tx.Model(&store.RHash{}).
			Where("kid = ? AND kfield IN ?", rkey.ID, fields).
			Delete(&store.RHash{})
		if result.Error != nil {
			return result.Error
		}
		n = result.RowsAffected

		if n > 0 {
			return tx.Model(&store.RKey{}).
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
func (d *DB) Exists(key, field string) (bool, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Count int64
	}
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", d.dbIdx, key, field).
		Scopes(store.NotExpired(now)).
		Count(&result.Count).Error

	return result.Count > 0, err
}

// Fields returns all fields in a hash.
func (d *DB) Fields(key string) ([]string, error) {
	now := time.Now().UnixMilli()
	var results []store.RHash
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	fields := make([]string, len(results))
	for i, r := range results {
		fields[i] = r.KField
	}
	return fields, nil
}

// Get returns the value of a field in a hash.
func (d *DB) Get(key, field string) (core.Value, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Value []byte
	}
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", d.dbIdx, key, field).
		Scopes(store.NotExpired(now)).
		Select("rhash.kval as value").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}
	return core.Value(result.Value), nil
}

// GetMany returns a map of values for given fields.
func (d *DB) GetMany(key string, fields ...string) (map[string]core.Value, error) {
	if len(fields) == 0 {
		return map[string]core.Value{}, nil
	}

	now := time.Now().UnixMilli()
	var results []store.RHash
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield IN ?", d.dbIdx, key, fields).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make(map[string]core.Value, len(results))
	for _, r := range results {
		items[r.KField] = core.Value(r.KVal)
	}
	return items, nil
}

// Incr increments the integer value of a field by delta.
// Returns the value after the increment.
func (d *DB) Incr(key, field string, delta int) (int, error) {
	newVal := 0
	item := hashIncrWriteItem{delta: delta, value: &newVal}
	err := d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key)+"\x00"+field,
		item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			incs := make([]hashIncrWriteItem, 0, len(items))
			total := 0
			for _, raw := range items {
				inc := raw.(hashIncrWriteItem)
				incs = append(incs, inc)
				total += inc.delta
			}
			if dialect == store.DialectPostgres {
				var result struct {
					KVal []byte `gorm:"column:kval"`
				}
				err := tx.Raw(`
				UPDATE rhash AS h
				SET kval = CONVERT_TO((CONVERT_FROM(h.kval, 'UTF8')::bigint + ?)::text, 'UTF8')
				FROM rkey AS k
				WHERE h.kid = k.id
				  AND k.kdb = ? AND k.kname = ? AND k.ktype = ? AND h.kfield = ?
				  AND CONVERT_FROM(h.kval, 'UTF8') ~ '^-?[0-9]+$'
				RETURNING h.kval`,
					total, d.dbIdx, key, core.TypeHash.Value(), field).Scan(&result).Error
				if err == nil && result.KVal != nil {
					final, convErr := strconv.Atoi(string(result.KVal))
					if convErr != nil {
						return core.ErrValueType
					}
					d.assignHashIncrResults(incs, final-total)
					return nil
				}
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
			}
			if dialect == store.DialectMySQL {
				result := tx.Exec(`
				UPDATE rhash AS h
				JOIN rkey AS k ON h.kid = k.id
				SET h.kval = CAST(CAST(h.kval AS SIGNED) + ? AS CHAR)
				WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ? AND h.kfield = ?
				  AND h.kval REGEXP '^-?[0-9]+$'`,
					total, d.dbIdx, key, core.TypeHash.Value(), field)
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected > 0 {
					var current struct {
						KVal []byte `gorm:"column:kval"`
					}
					err := tx.Model(&store.RKey{}).
						Select("rhash.kval").
						Joins("JOIN rhash ON rhash.kid = rkey.id AND rhash.kfield = ?", field).
						Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
						First(&current).Error
					if err != nil {
						return err
					}
					final, convErr := strconv.Atoi(string(current.KVal))
					if convErr != nil {
						return core.ErrValueType
					}
					d.assignHashIncrResults(incs, final-total)
					return nil
				}
			}

			var current struct {
				ID    int    `gorm:"column:id"`
				KType int    `gorm:"column:ktype"`
				KVal  []byte `gorm:"column:kval"`
			}
			currentQuery := tx.Model(&store.RKey{}).
				Select("rkey.id, rkey.ktype, rhash.kval").
				Joins("JOIN rhash ON rhash.kid = rkey.id AND rhash.kfield = ?", field).
				Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				currentQuery = currentQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			if err := currentQuery.First(&current).Error; err == nil {
				if current.KType != core.TypeHash.Value() {
					return core.ErrKeyType
				}
				valInt, convErr := strconv.Atoi(string(current.KVal))
				if convErr != nil {
					return core.ErrValueType
				}
				final := d.assignHashIncrResults(incs, valInt)
				return tx.Model(&store.RHash{}).
					Where("kid = ? AND kfield = ?", current.ID, field).
					Update("kval", []byte(strconv.Itoa(final))).Error
			} else if !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
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
					KType:      core.TypeHash.Value(),
					KVer:       1,
					ModifiedAt: now,
					KLen:       1,
				}
				if err := tx.Create(&rkey).Error; err != nil {
					return err
				}
				final := d.assignHashIncrResults(incs, 0)
				rhash := store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   []byte(strconv.Itoa(final)),
				}
				return tx.Create(&rhash).Error

			case err != nil:
				return err

			case rkey.KType != core.TypeHash.Value():
				return core.ErrKeyType

			default:
				var rhash store.RHash
				err = tx.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&rhash).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					final := d.assignHashIncrResults(incs, 0)
					rhash := store.RHash{
						KID:    rkey.ID,
						KField: field,
						KVal:   []byte(strconv.Itoa(final)),
					}
					if err := tx.Create(&rhash).Error; err != nil {
						return err
					}
					return tx.Model(&store.RKey{}).
						Where("id = ?", rkey.ID).
						Updates(map[string]interface{}{
							"kver":        gorm.Expr("kver + 1"),
							"modified_at": now,
							"klen":        gorm.Expr("klen + 1"),
						}).Error
				}
				if err != nil {
					return err
				}

				valInt, err := strconv.Atoi(string(rhash.KVal))
				if err != nil {
					return core.ErrValueType
				}
				final := d.assignHashIncrResults(incs, valInt)

				rhash.KVal = []byte(strconv.Itoa(final))
				if err := tx.Save(&rhash).Error; err != nil {
					return err
				}
				return tx.Model(&store.RKey{}).
					Where("id = ?", rkey.ID).
					Updates(map[string]interface{}{
						"kver":        gorm.Expr("kver + 1"),
						"modified_at": now,
					}).Error
			}
		})

	return newVal, err
}

func (d *DB) assignHashIncrResults(items []hashIncrWriteItem, start int) int {
	current := start
	for _, item := range items {
		current += item.delta
		*item.value = current
	}
	return current
}

type hashIncrWriteItem struct {
	delta int
	value *int
}

// IncrFloat increments the float value of a field by delta.
func (d *DB) IncrFloat(key, field string, delta float64) (float64, error) {
	newValue := 0.0
	item := hashIncrFloatWriteItem{delta: delta, value: &newValue}
	err := d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key)+"\x00"+field,
		item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			incs := make([]hashIncrFloatWriteItem, 0, len(items))
			for _, raw := range items {
				incs = append(incs, raw.(hashIncrFloatWriteItem))
			}

			var current struct {
				ID       int    `gorm:"column:id"`
				KType    int    `gorm:"column:ktype"`
				KVal     []byte `gorm:"column:kval"`
				HasValue bool   `gorm:"column:has_value"`
			}
			query := tx.Model(&store.RKey{}).
				Select("rkey.id, rkey.ktype, rhash.kval, rhash.kid IS NOT NULL AS has_value").
				Joins("LEFT JOIN rhash ON rhash.kid = rkey.id AND rhash.kfield = ?", field).
				Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
			// PostgreSQL cannot lock the nullable side of this outer join.
			if dialect == store.DialectMySQL {
				query = query.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			err := query.First(&current).Error
			createdKey := false
			if errors.Is(err, gorm.ErrRecordNotFound) {
				rkey, keyCreated, err := store.InsertKeyIfAbsent(tx, dialect, d.dbIdx, key,
					core.TypeHash.Value(), now, 1)
				if err != nil {
					return err
				}
				current.ID = rkey.ID
				current.KType = rkey.KType
				createdKey = keyCreated
			} else if err != nil {
				return err
			}
			if current.KType != core.TypeHash.Value() {
				return core.ErrKeyType
			}

			start := 0.0
			if current.HasValue {
				start, err = strconv.ParseFloat(string(current.KVal), 64)
				if err != nil {
					return core.ErrValueType
				}
			}
			final := d.assignHashIncrFloatResults(incs, start)
			value := []byte(strconv.FormatFloat(final, 'f', -1, 64))

			if current.HasValue {
				if dialect == store.DialectMySQL {
					return tx.Exec(
						"UPDATE rhash AS h JOIN rkey AS k ON k.id = h.kid "+
							"SET h.kval = ?, k.kver = k.kver + ?, k.modified_at = ? "+
							"WHERE h.kid = ? AND h.kfield = ?",
						value, len(items), now, current.ID, field,
					).Error
				}
				if err := tx.Model(&store.RHash{}).
					Where("kid = ? AND kfield = ?", current.ID, field).
					Update("kval", value).Error; err != nil {
					return err
				}
			} else {
				row := store.RHash{KID: current.ID, KField: field, KVal: value}
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
				if !createdKey {
					return tx.Model(&store.RKey{}).
						Where("id = ?", current.ID).
						Updates(map[string]interface{}{
							"kver":        gorm.Expr("kver + 1"),
							"modified_at": now,
							"klen":        gorm.Expr("klen + 1"),
						}).Error
				}
				return nil
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

func (d *DB) assignHashIncrFloatResults(items []hashIncrFloatWriteItem, start float64) float64 {
	current := start
	for _, item := range items {
		current += item.delta
		*item.value = current
	}
	return current
}

type hashIncrFloatWriteItem struct {
	delta float64
	value *float64
}

// Len returns the number of fields in a hash.
func (d *DB) Len(key string) (int, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Count int64
	}
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Count(&result.Count).Error

	return int(result.Count), err
}

// Random returns a random field from a hash.
func (d *DB) Random(key string) (HashItem, error) {
	now := time.Now().UnixMilli()
	var rhash store.RHash
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now), store.RandomOrder(d.store.Dialect)).
		First(&rhash).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return HashItem{}, core.ErrNotFound
	}
	if err != nil {
		return HashItem{}, err
	}
	return HashItem{Field: rhash.KField, Value: core.Value(rhash.KVal)}, nil
}

// RandFields returns count random fields from a hash.
func (d *DB) RandFields(key string, count int) ([]HashItem, error) {
	now := time.Now().UnixMilli()
	var results []store.RHash

	query := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now))

	if count >= 0 {
		query = query.Limit(count)
	} else {
		query = query.Limit(-count)
	}

	err := query.Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make([]HashItem, len(results))
	for i, r := range results {
		items[i] = HashItem{Field: r.KField, Value: core.Value(r.KVal)}
	}

	if count < 0 && len(items) > 0 {
		rand.Shuffle(len(items), func(i, j int) {
			items[i], items[j] = items[j], items[i]
		})
	}

	return items, nil
}

// Scan iterates over hash fields matching pattern.
// Uses database-level pagination for better performance and consistency.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count <= 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()
	var results []store.RHash

	// Use database-level pagination with cursor-based approach
	query := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Order("rhash.id ASC"). // Ensure consistent ordering
		Limit(count)

	// Cursor-based pagination: get records with ID > cursor
	if cursor > 0 {
		query = query.Where("rhash.id > ?", cursor)
	}

	err := query.Find(&results).Error
	if err != nil {
		return ScanResult{}, err
	}

	// Convert results
	items := make([]HashItem, len(results))
	var nextCursor int
	for i, r := range results {
		items[i] = HashItem{Field: r.KField, Value: core.Value(r.KVal)}
		nextCursor = r.ID // Use last ID as next cursor
	}

	// If we got fewer results than requested, we've reached the end
	if len(results) < count {
		nextCursor = 0
	}

	return ScanResult{Cursor: nextCursor, Items: items}, nil
}

// Scanner returns an iterator over hash items matching pattern.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	if pageSize <= 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		db:       d,
		key:      key,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
	}
}

// Next returns the next hash item.
func (s *Scanner) Next() bool {
	if s.err != nil || s.cursor == -1 {
		return false
	}

	if s.index < len(s.items) {
		s.cur = s.items[s.index]
		s.index++
		return true
	}

	if s.cursor == 0 && len(s.items) == 0 && s.index == 0 {
		result, err := s.db.Scan(s.key, 0, s.pattern, s.pageSize)
		if err != nil {
			s.err = err
			return false
		}
		s.items = result.Items
		s.cursor = result.Cursor
		s.index = 0

		if len(s.items) == 0 {
			s.cursor = -1
			return false
		}

		s.cur = s.items[s.index]
		s.index++
		return true
	}

	if s.cursor == 0 {
		s.cursor = -1
		return false
	}

	result, err := s.db.Scan(s.key, s.cursor, s.pattern, s.pageSize)
	if err != nil {
		s.err = err
		return false
	}
	s.items = result.Items
	s.cursor = result.Cursor
	s.index = 0

	if len(s.items) == 0 {
		s.cursor = -1
		return false
	}

	s.cur = s.items[s.index]
	s.index++
	return true
}

// Value returns the current hash item.
func (s *Scanner) Value() HashItem {
	return s.cur
}

// Err returns the last error encountered.
func (s *Scanner) Err() error {
	return s.err
}

// Set sets the value of a field in a hash.
// Returns true if the field was created, false if it was updated.
func (d *DB) Set(key, field string, value any) (bool, error) {
	vb, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}
	created := false
	item := hashWriteItem{field: field, value: vb, created: &created}
	err = d.store.TransactionWriteItems(context.Background(),
		d.writeScope(key)+"\x00"+field, item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			last := items[len(items)-1].(hashWriteItem)
			current, getErr := d.getInDB(tx, key, last.field)
			existed := getErr == nil
			if getErr != nil && !errors.Is(getErr, core.ErrNotFound) {
				return getErr
			}
			if existed {
				if bytes.Equal(current, last.value) {
					return nil
				}
				_, updateErr := d.updateExisting(tx, key, last.field, last.value)
				return updateErr
			}

			rowCreated, setErr := d.setNonBatched(tx, key, last.field, last.value)
			if setErr != nil {
				return setErr
			}
			if rowCreated && len(items) > 0 {
				*items[0].(hashWriteItem).created = true
			}
			return nil
		})
	return created, err
}

type hashWriteItem struct {
	field   string
	value   []byte
	created *bool
}

func (d *DB) getInDB(db *gorm.DB, key, field string) ([]byte, error) {
	var result struct {
		Value []byte
	}
	err := db.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", d.dbIdx, key, field).
		Scopes(store.NotExpired(time.Now().UnixMilli())).
		Select("rhash.kval as value").
		First(&result).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return result.Value, nil
}

func (d *DB) setNonBatched(db *gorm.DB, key, field string, vb []byte) (bool, error) {
	var created bool
	err := db.Transaction(func(tx *gorm.DB) error {
		dialect := d.store.Dialect
		now := time.Now().UnixMilli()
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
			core.TypeHash.Value(), now, 0)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeHash.Value() {
			return core.ErrKeyType
		}

		row := store.RHash{KID: rkey.ID, KField: field, KVal: vb}
		result := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "kfield"}}, &row)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected == 1
		if created {
			return nil
		}
		return tx.Model(&store.RHash{}).
			Where("kid = ? AND kfield = ?", rkey.ID, field).
			Update("kval", vb).Error
	})
	return created, err
}

func (d *DB) writeScope(key string) string {
	return "hash:write:" + strconv.Itoa(d.dbIdx) + "\x00" + key
}

// updateExisting performs a one-statement update of an existing hot field.
func (d *DB) updateExisting(db *gorm.DB, key, field string, value []byte) (bool, error) {
	if d.store.Dialect == store.DialectMySQL {
		result := db.Exec(`
			UPDATE rkey AS k
			JOIN rhash AS h ON h.kid = k.id
			SET h.kval = ?
			WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ? AND h.kfield = ?`,
			value, d.dbIdx, key, core.TypeHash.Value(), field)
		if result.Error != nil {
			return false, result.Error
		}
		return result.RowsAffected > 0, nil
	}

	var result *gorm.DB
	switch d.store.Dialect {
	case store.DialectSQLite, store.DialectPostgres:
		result = db.Exec(`
			UPDATE rhash
			SET kval = ?
			WHERE kid = (
				SELECT id FROM rkey
				WHERE kdb = ? AND kname = ? AND ktype = ?
			) AND kfield = ?`,
			value, d.dbIdx, key, core.TypeHash.Value(), field)
	default:
		return false, nil
	}
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// SetMany sets multiple field-value pairs in a hash.
// Returns the number of fields that were added (as opposed to updated).
func (d *DB) SetMany(key string, items map[string]any) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	values := make(map[string][]byte, len(items))
	for k, v := range items {
		vb, err := core.ToBytes(v)
		if err != nil {
			return 0, err
		}
		values[k] = vb
	}

	if len(values) == 1 {
		for field, val := range values {
			created, err := d.Set(key, field, val)
			if err != nil {
				return 0, err
			}
			if created {
				return 1, nil
			}
			return 0, nil
		}
	}

	var newCount int
	err := d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
		now := time.Now().UnixMilli()
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key, core.TypeHash.Value(), now, 0)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeHash.Value() {
			return core.ErrKeyType
		}

		rows := make([]store.RHash, 0, len(values))
		fields := make([]string, 0, len(values))
		caseArgs := make([]interface{}, 0, len(values)*2)
		for field, val := range values {
			rows = append(rows, store.RHash{KID: rkey.ID, KField: field, KVal: val})
			fields = append(fields, field)
			caseArgs = append(caseArgs, field, val)
		}

		result := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "kfield"}}, &rows)
		if result.Error != nil {
			return result.Error
		}
		newCount = int(result.RowsAffected)
		if newCount == len(values) {
			return nil
		}

		// Existing fields need their values set; newly inserted fields already
		// have the requested value and are unchanged by the matching branch.
		return tx.Model(&store.RHash{}).
			Where("kid = ? AND kfield IN ?", rkey.ID, fields).
			Update("kval", gorm.Expr(
				"CASE kfield "+strings.Repeat("WHEN ? THEN ? ", len(values))+"ELSE kval END",
				caseArgs...,
			)).Error
	})

	return newCount, err
}

// StrLen returns the length of the value of a field.
func (d *DB) StrLen(key, field string) (int, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Len int
	}
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rhash.kfield = ?", d.dbIdx, key, field).
		Scopes(store.NotExpired(now)).
		Select("LENGTH(rhash.kval) as len").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return result.Len, nil
}

// Values returns all values in a hash.
func (d *DB) Values(key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	var results []store.RHash
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	values := make([]core.Value, len(results))
	for i, r := range results {
		values[i] = core.Value(r.KVal)
	}
	return values, nil
}

// Items returns all field-value pairs in a hash as a map.
func (d *DB) Items(key string) (map[string]core.Value, error) {
	now := time.Now().UnixMilli()
	var results []store.RHash
	err := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make(map[string]core.Value, len(results))
	for _, r := range results {
		items[r.KField] = core.Value(r.KVal)
	}
	return items, nil
}

// RandField returns one or more random fields from a hash.
func (d *DB) RandField(key string, count int, withValues bool) ([]HashItem, error) {
	now := time.Now().UnixMilli()
	var results []store.RHash

	query := d.store.DB.Model(&store.RHash{}).
		Joins("JOIN rkey ON rhash.kid = rkey.id AND rkey.ktype = ?", core.TypeHash.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now), store.RandomOrder(d.store.Dialect))

	if count >= 0 {
		query = query.Limit(count)
	} else {
		query = query.Limit(-count)
	}

	err := query.Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make([]HashItem, len(results))
	for i, r := range results {
		items[i] = HashItem{Field: r.KField, Value: core.Value(r.KVal)}
	}

	if count < 0 && len(items) > 0 {
		rand.Shuffle(len(items), func(i, j int) {
			items[i], items[j] = items[j], items[i]
		})
	}

	return items, nil
}

// SetNotExists sets the value of a field only if the field does not exist.
// Returns true if the field was set, false if it already existed.
func (d *DB) SetNotExists(key, field string, value any) (bool, error) {
	vb, err := core.ToBytes(value)
	if err != nil {
		return false, err
	}

	var created bool
	err = d.store.TransactionRetry(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
		now := time.Now().UnixMilli()
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key, core.TypeHash.Value(), now, 0)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeHash.Value() {
			return core.ErrKeyType
		}

		row := store.RHash{KID: rkey.ID, KField: field, KVal: vb}
		result := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "kfield"}}, &row)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected == 1
		return nil
	})

	return created, err
}
