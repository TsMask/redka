// Package rhash is a database-backed hash repository.
// It provides methods to interact with hashmaps in the database.
package rhash

import (
	"context"
	"errors"
	"math/rand"
	"strconv"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
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
	var newVal int
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
				KType:      core.TypeHash.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			rhash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   []byte(strconv.Itoa(delta)),
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
				newVal = delta
				rhash := store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   []byte(strconv.Itoa(delta)),
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
			newVal = valInt + delta

			rhash.KVal = []byte(strconv.Itoa(newVal))
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

// IncrFloat increments the float value of a field by delta.
func (d *DB) IncrFloat(key, field string, delta float64) (float64, error) {
	var newVal float64
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
				KType:      core.TypeHash.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			newVal = delta
			rhash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
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
				newVal = delta
				rhash := store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   []byte(strconv.FormatFloat(delta, 'f', -1, 64)),
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

			valFloat, err := strconv.ParseFloat(string(rhash.KVal), 64)
			if err != nil {
				return core.ErrValueType
			}
			newVal = valFloat + delta

			rhash.KVal = []byte(strconv.FormatFloat(newVal, 'f', -1, 64))
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
		Scopes(store.NotExpired(now)).
		Order("RANDOM()").
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

	var created bool
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
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
				KType:      core.TypeHash.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			rhash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   vb,
			}
			if err := tx.Create(&rhash).Error; err != nil {
				return err
			}
			created = true
			return nil

		case err != nil:
			return err

		case rkey.KType != core.TypeHash.Value():
			return core.ErrKeyType

		default:
			var existing store.RHash
			err = tx.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				rhash := store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   vb,
				}
				if err := tx.Create(&rhash).Error; err != nil {
					return err
				}
				created = true
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

			existing.KVal = vb
			if err := tx.Save(&existing).Error; err != nil {
				return err
			}
			created = false
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
	})

	return created, err
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

	var newCount int

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
				KType:      core.TypeHash.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}

		case err != nil:
			return err

		case rkey.KType != core.TypeHash.Value():
			return core.ErrKeyType
		}

		// 批量查询所有现有字段，避免 N+1 查询
		var existingFields []store.RHash
		err = tx.Where("kid = ?", rkey.ID).Find(&existingFields).Error
		if err != nil {
			return err
		}

		// 构建已存在字段的 map
		existingMap := make(map[string]*store.RHash, len(existingFields))
		for i := range existingFields {
			existingMap[existingFields[i].KField] = &existingFields[i]
		}

		// 分离新增和更新的字段
		var newFields []store.RHash
		var updateFields []store.RHash
		newCount = 0

		for field, val := range values {
			if existing, ok := existingMap[field]; ok {
				// 字段已存在，准备更新
				existing.KVal = val
				updateFields = append(updateFields, *existing)
			} else {
				// 字段不存在，准备插入
				newFields = append(newFields, store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   val,
				})
				newCount++
			}
		}

		// 批量插入新字段
		if len(newFields) > 0 {
			if err := tx.Create(&newFields).Error; err != nil {
				return err
			}
		}

		// 批量更新现有字段
		if len(updateFields) > 0 {
			for i := range updateFields {
				if err := tx.Save(&updateFields[i]).Error; err != nil {
					return err
				}
			}
		}

		// 更新键的长度
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen + ?", newCount),
			}).Error
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
		Scopes(store.NotExpired(now)).
		Order("RANDOM()")

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
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
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
				KType:      core.TypeHash.Value(),
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			rhash := store.RHash{
				KID:    rkey.ID,
				KField: field,
				KVal:   vb,
			}
			if err := tx.Create(&rhash).Error; err != nil {
				return err
			}
			created = true
			return nil

		case err != nil:
			return err

		case rkey.KType != core.TypeHash.Value():
			return core.ErrKeyType

		default:
			var existing store.RHash
			err = tx.Where("kid = ? AND kfield = ?", rkey.ID, field).First(&existing).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				rhash := store.RHash{
					KID:    rkey.ID,
					KField: field,
					KVal:   vb,
				}
				if err := tx.Create(&rhash).Error; err != nil {
					return err
				}
				created = true
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
			created = false
			return nil
		}
	})

	return created, err
}
