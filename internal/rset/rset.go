// Package rset is a database-backed set repository.
// It provides methods to interact with sets in the database.
package rset

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
)

// ScanResult represents the result of a scan operation.
type ScanResult struct {
	Cursor int
	Items  []core.Value
}

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

// keyTypeSet is the type ID for set keys.
const keyTypeSet = 3

// DB is a database-backed set repository.
// A set is an unordered collection of unique strings.
// Use the set repository to work with individual sets
// and their elements, and to perform set operations
// like union, intersection.
//
// This is a simplified architecture that directly uses store.Store
// without additional transaction wrappers. Each method handles
// its own transactions internally when needed.
type DB struct {
	store *store.Store
	dbIdx int
}

// Scanner is the iterator for set items.
// Stops when there are no more items or an error occurs.
type Scanner struct {
	db       *DB
	key      string
	cursor   int
	pattern  string
	pageSize int
	index    int
	cur      core.Value
	items    []core.Value
	err      error
}

// New connects to the set repository.
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

// Add adds or updates elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	newCount := 0

	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		switch {
		case err == nil:
			if rkey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			if err := tx.Model(&rkey).Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error; err != nil {
				return err
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// 批量查询已存在的元素，避免 N+1 查询
		var existingElems []store.RSet
		err = tx.Where("kid = ? AND elem IN ?", rkey.ID, elembs).Find(&existingElems).Error
		if err != nil {
			return err
		}

		// 构建已存在元素的集合
		existMap := make(map[string]bool, len(existingElems))
		for _, e := range existingElems {
			existMap[string(e.Elem)] = true
		}

		// 批量插入新元素
		var newElems []store.RSet
		for _, elem := range elembs {
			if !existMap[string(elem)] {
				newElems = append(newElems, store.RSet{KID: rkey.ID, Elem: elem})
				newCount++
			}
		}

		if len(newElems) > 0 {
			if err := tx.Create(&newElems).Error; err != nil {
				return err
			}
		}

		// 更新键的长度
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Update("klen", gorm.Expr("klen + ?", newCount)).Error
	})

	return newCount, err
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	var n int
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			n = 0
			return nil
		}
		if err != nil {
			return err
		}

		result := tx.Where("kid = ? AND elem IN ?", rkey.ID, elembs).
			Delete(&store.RSet{})
		if result.Error != nil {
			return result.Error
		}

		n = int(result.RowsAffected)
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

	return n, err
}

// Diff returns the difference between the first set and the rest.
func (d *DB) Diff(keys ...string) ([]core.Value, error) {
	return d.diffTx(d.store.DB, keys...)
}

// diffTx computes the difference of sets within a transaction.
// Used internally by DiffStore to ensure atomicity.
func (d *DB) diffTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	now := time.Now().UnixMilli()
	var results []store.RSet

	err := db.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, keys[0]).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return []core.Value{}, nil
	}

	elems := make(map[string]bool)
	for _, r := range results {
		elems[string(r.Elem)] = true
	}

	for _, key := range keys[1:] {
		var results []store.RSet
		err := db.Model(&store.RSet{}).
			Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
			Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Find(&results).Error
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			delete(elems, string(r.Elem))
		}
	}

	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items, nil
}

// DiffStore calculates the difference and stores the result in a destination set.
// The difference calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) DiffStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	var resultCount int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		// Calculate difference within the same transaction for atomicity
		items, err := d.diffTx(tx, keys...)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if err == nil && rkey.KType != keyTypeSet {
			return core.ErrKeyType
		}

		if err := tx.Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Delete(&store.RKey{}).Error; err != nil {
			return err
		}

		if len(items) == 0 {
			return nil
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      dest,
			KType:      keyTypeSet,
			KVer:       1,
			ModifiedAt: now,
			KLen:       len(items),
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rsets := make([]store.RSet, len(items))
		for i, item := range items {
			rsets[i] = store.RSet{KID: rkey.ID, Elem: []byte(item)}
		}
		if err := tx.Create(&rsets).Error; err != nil {
			return err
		}

		resultCount = len(items)
		return nil
	})

	return resultCount, err
}

// Exists reports whether the element belongs to a set.
func (d *DB) Exists(key string, elem any) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()
	var result struct {
		Count int64
	}
	err = d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rset.elem = ?", d.dbIdx, key, elemb).
		Scopes(store.NotExpired(now)).
		Count(&result.Count).Error

	return result.Count > 0, err
}

// Inter returns the intersection of multiple sets.
func (d *DB) Inter(keys ...string) ([]core.Value, error) {
	return d.interTx(d.store.DB, keys...)
}

// interTx computes the intersection of sets within a transaction.
// Used internally by InterStore to ensure atomicity.
func (d *DB) interTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	now := time.Now().UnixMilli()

	var results []store.RSet
	err := db.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, keys[0]).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	if len(results) == 0 {
		return []core.Value{}, nil
	}

	elems := make(map[string]bool)
	for _, r := range results {
		elems[string(r.Elem)] = true
	}

	for _, key := range keys[1:] {
		var results []store.RSet
		err := db.Model(&store.RSet{}).
			Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
			Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Find(&results).Error
		if err != nil {
			return nil, err
		}

		temp := make(map[string]bool)
		for _, r := range results {
			if elems[string(r.Elem)] {
				temp[string(r.Elem)] = true
			}
		}
		elems = temp
		if len(elems) == 0 {
			break
		}
	}

	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items, nil
}

// InterStore intersects multiple sets and stores the result in a destination set.
// The intersection calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) InterStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	var resultCount int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		// Calculate intersection within the same transaction for atomicity
		items, err := d.interTx(tx, keys...)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if err == nil && rkey.KType != keyTypeSet {
			return core.ErrKeyType
		}

		if err := tx.Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Delete(&store.RKey{}).Error; err != nil {
			return err
		}

		if len(items) == 0 {
			return nil
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      dest,
			KType:      keyTypeSet,
			KVer:       1,
			ModifiedAt: now,
			KLen:       len(items),
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rsets := make([]store.RSet, len(items))
		for i, item := range items {
			rsets[i] = store.RSet{KID: rkey.ID, Elem: []byte(item)}
		}
		if err := tx.Create(&rsets).Error; err != nil {
			return err
		}

		resultCount = len(items)
		return nil
	})

	return resultCount, err
}

// InterCard returns the number of elements in the intersection of multiple sets.
func (d *DB) InterCard(limit int, keys ...string) (int, error) {
	items, err := d.Inter(keys...)
	if err != nil {
		return 0, err
	}

	if limit > 0 && len(items) > limit {
		return limit, nil
	}
	return len(items), nil
}

// ExistsMany checks if multiple members exist in a set.
func (d *DB) ExistsMany(key string, members ...any) ([]bool, error) {
	if len(members) == 0 {
		return []bool{}, nil
	}

	elembs, err := core.ToBytesMany(members...)
	if err != nil {
		return nil, err
	}

	now := time.Now().UnixMilli()
	var results []store.RSet
	err = d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rset.elem IN ?", d.dbIdx, key, elembs).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	existMap := make(map[string]bool)
	for _, r := range results {
		existMap[string(r.Elem)] = true
	}

	results2 := make([]bool, len(members))
	for i, m := range members {
		mb, _ := core.ToBytes(m)
		results2[i] = existMap[string(mb)]
	}
	return results2, nil
}

// Items returns all elements in a set.
func (d *DB) Items(key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	var results []store.RSet
	err := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make([]core.Value, len(results))
	for i, r := range results {
		items[i] = core.Value(r.Elem)
	}
	return items, nil
}

// Len returns the number of elements in a set.
func (d *DB) Len(key string) (int, error) {
	now := time.Now().UnixMilli()
	var result struct {
		Count int64
	}
	err := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Count(&result.Count).Error

	return int(result.Count), err
}

// Move moves an element from one set to another.
func (d *DB) Move(src, dest string, elem any) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	var moved bool
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		// 死锁预防：按键名排序后加锁，确保固定的锁定顺序
		// 这样可以避免两个并发事务以相反顺序锁定相同的键而导致死锁
		keys := []string{src, dest}
		sort.Strings(keys)
		firstKey := keys[0]
		secondKey := keys[1]

		// 按排序后的顺序锁定键
		var firstKeyRecord store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, firstKey).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&firstKeyRecord).Error

		// 根据 firstKey 是 src 还是 dest 来处理
		var srcKey store.RKey
		var destKey store.RKey

		if firstKey == src {
			// firstKey 是 src
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}
			if firstKeyRecord.KType != keyTypeSet {
				return core.ErrKeyType
			}
			srcKey = firstKeyRecord

			// 检查 dest 是否存在
			err = tx.Model(&store.RKey{}).
				Where("kdb = ? AND kname = ?", d.dbIdx, secondKey).
				Scopes(store.NotExpired(now)).
				Clauses(store.ForUpdate()).
				First(&destKey).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				destKey = store.RKey{
					KDB:        d.dbIdx,
					KName:      dest,
					KType:      keyTypeSet,
					KVer:       1,
					ModifiedAt: now,
					KLen:       0,
				}
				if err := tx.Create(&destKey).Error; err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
		} else {
			// firstKey 是 dest
			if errors.Is(err, gorm.ErrRecordNotFound) {
				// dest 不存在，创建它
				destKey = store.RKey{
					KDB:        d.dbIdx,
					KName:      dest,
					KType:      keyTypeSet,
					KVer:       1,
					ModifiedAt: now,
					KLen:       0,
				}
				if err := tx.Create(&destKey).Error; err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			destKey = firstKeyRecord

			// 检查 src 是否存在
			err = tx.Model(&store.RKey{}).
				Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, secondKey, keyTypeSet).
				Scopes(store.NotExpired(now)).
				Clauses(store.ForUpdate()).
				First(&srcKey).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}
		}

		// 检查源集合中是否存在该元素
		var srcSet store.RSet
		err = tx.Where("kid = ? AND elem = ?", srcKey.ID, elemb).First(&srcSet).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		// 检查目标集合中是否已存在该元素
		var existing store.RSet
		err = tx.Where("kid = ? AND elem = ?", destKey.ID, elemb).First(&existing).Error
		if err == nil {
			// 元素已存在于目标集合，只需从源集合删除
			if err := tx.Where("kid = ? AND elem = ?", srcKey.ID, elemb).
				Delete(&store.RSet{}).Error; err != nil {
				return err
			}
			moved = true
			return tx.Model(&store.RKey{}).
				Where("id = ?", srcKey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen - 1"),
				}).Error
		}
		if !errors.Is(err, gorm.ErrRecordNotFound) {
			return err
		}

		// 元素不存在于目标集合，移动元素
		if err := tx.Create(&store.RSet{KID: destKey.ID, Elem: elemb}).Error; err != nil {
			return err
		}
		if err := tx.Where("kid = ? AND elem = ?", srcKey.ID, elemb).
			Delete(&store.RSet{}).Error; err != nil {
			return err
		}

		// 更新两个键的长度
		if err := tx.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Update("klen", gorm.Expr("klen + 1")).Error; err != nil {
			return err
		}

		moved = true
		return tx.Model(&store.RKey{}).
			Where("id = ?", srcKey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			}).Error
	})

	return moved, err
}

// Pop removes and returns a random element from a set.
func (d *DB) Pop(key string) (core.Value, error) {
	var elem core.Value
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		var rset store.RSet
		err = tx.Where("kid = ?", rkey.ID).First(&rset).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		elem = core.Value(rset.Elem)
		if err := tx.Where("kid = ? AND elem = ?", rkey.ID, rset.Elem).
			Delete(&store.RSet{}).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			}).Error
	})

	return elem, err
}

// Random returns a random element from a set.
func (d *DB) Random(key string) (core.Value, error) {
	now := time.Now().UnixMilli()
	var rset store.RSet
	err := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Order("RANDOM()").
		First(&rset).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return core.Value{}, core.ErrNotFound
	}
	if err != nil {
		return core.Value{}, err
	}
	return core.Value(rset.Elem), nil
}

// RandMember returns multiple random elements from a set.
func (d *DB) RandMember(key string, count int) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	var rsets []store.RSet

	query := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now))

	if count >= 0 {
		query = query.Limit(count)
	} else {
		query = query.Limit(-count)
	}

	err := query.Find(&rsets).Error
	if err != nil {
		return nil, err
	}

	items := make([]core.Value, len(rsets))
	for i, r := range rsets {
		items[i] = core.Value(r.Elem)
	}

	if count < 0 && len(items) > 0 {
		rand.Shuffle(len(items), func(i, j int) {
			items[i], items[j] = items[j], items[i]
		})
	}

	return items, nil
}

// Scan iterates over set elements matching pattern.
// Uses database-level pagination for better performance and consistency.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count <= 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()
	var results []store.RSet

	// Use database-level pagination with cursor-based approach
	query := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Order("rset.id ASC"). // Ensure consistent ordering
		Limit(count)

	// Cursor-based pagination: get records with ID > cursor
	if cursor > 0 {
		query = query.Where("rset.id > ?", cursor)
	}

	err := query.Find(&results).Error
	if err != nil {
		return ScanResult{}, err
	}

	// Convert results
	items := make([]core.Value, len(results))
	var nextCursor int
	for i, r := range results {
		items[i] = core.Value(r.Elem)
		nextCursor = r.ID // Use last ID as next cursor
	}

	// If we got fewer results than requested, we've reached the end
	if len(results) < count {
		nextCursor = 0
	}

	return ScanResult{Cursor: nextCursor, Items: items}, nil
}

// Scanner returns an iterator over set elements matching pattern.
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

// Next returns the next element in the set.
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

// Value returns the current element.
func (s *Scanner) Value() core.Value {
	return s.cur
}

// Err returns the last error encountered.
func (s *Scanner) Err() error {
	return s.err
}

// Union returns the union of multiple sets.
func (d *DB) Union(keys ...string) ([]core.Value, error) {
	return d.unionTx(d.store.DB, keys...)
}

// unionTx computes the union of sets within a transaction.
// Used internally by UnionStore to ensure atomicity.
func (d *DB) unionTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	now := time.Now().UnixMilli()
	elems := make(map[string]bool)

	for _, key := range keys {
		var results []store.RSet
		err := db.Model(&store.RSet{}).
			Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", keyTypeSet).
			Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Find(&results).Error
		if err != nil {
			return nil, err
		}
		for _, r := range results {
			elems[string(r.Elem)] = true
		}
	}

	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items, nil
}

// UnionStore unions multiple sets and stores the result in a destination set.
// The union calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) UnionStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	var resultCount int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		// Calculate union within the same transaction for atomicity
		items, err := d.unionTx(tx, keys...)
		if err != nil {
			return err
		}

		var rkey store.RKey
		err = tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if err == nil && rkey.KType != keyTypeSet {
			return core.ErrKeyType
		}

		if err := tx.Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Delete(&store.RKey{}).Error; err != nil {
			return err
		}

		if len(items) == 0 {
			return nil
		}

		rkey = store.RKey{
			KDB:        d.dbIdx,
			KName:      dest,
			KType:      keyTypeSet,
			KVer:       1,
			ModifiedAt: now,
			KLen:       len(items),
		}
		if err := tx.Create(&rkey).Error; err != nil {
			return err
		}

		rsets := make([]store.RSet, len(items))
		for i, item := range items {
			rsets[i] = store.RSet{KID: rkey.ID, Elem: []byte(item)}
		}
		if err := tx.Create(&rsets).Error; err != nil {
			return err
		}

		resultCount = len(items)
		return nil
	})

	return resultCount, err
}
