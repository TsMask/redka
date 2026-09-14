// Package rset is a database-backed set repository.
// It provides methods to interact with sets in the database.
package rset

import (
	"context"
	"errors"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ScanResult represents the result of a scan operation.
type ScanResult struct {
	Cursor int
	Items  []core.Value
}

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

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

	if len(elembs) == 1 {
		exists, err := d.Exists(key, elems[0])
		if err != nil {
			return 0, err
		}
		if exists {
			return 0, nil
		}
	}

	var newCount int
	item := setAddWriteItem{elems: elembs, count: &newCount}
	err = d.store.TransactionWriteItems(context.Background(),
		"set:add:"+strconv.Itoa(d.dbIdx)+"\x00"+key, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			rkey, err := d.lockOrCreateSetKey(tx, dialect, key, now)
			if err != nil {
				return err
			}
			if rkey.KType != core.TypeSet.Value() {
				return core.ErrKeyType
			}

			expired := rkey.ExpireAt != nil && *rkey.ExpireAt <= now
			if expired {
				if err := tx.Where("kid = ?", rkey.ID).
					Delete(&store.RSet{}).Error; err != nil {
					return err
				}
			}

			uniqueElems := make(map[string]struct{})
			for _, raw := range items {
				for _, elem := range raw.(setAddWriteItem).elems {
					uniqueElems[string(elem)] = struct{}{}
				}
			}
			allElems := make([][]byte, 0, len(uniqueElems))
			for elem := range uniqueElems {
				allElems = append(allElems, []byte(elem))
			}

			var existing []store.RSet
			if err := tx.Where("kid = ? AND elem IN ?", rkey.ID, allElems).
				Find(&existing).Error; err != nil {
				return err
			}
			existingElems := make(map[string]struct{}, len(existing))
			for _, row := range existing {
				existingElems[string(row.Elem)] = struct{}{}
			}

			newElems := make(map[string]struct{})
			for _, raw := range items {
				add := raw.(setAddWriteItem)
				seen := make(map[string]struct{}, len(add.elems))
				count := 0
				for _, elem := range add.elems {
					name := string(elem)
					if _, ok := seen[name]; ok {
						continue
					}
					seen[name] = struct{}{}
					if _, ok := existingElems[name]; ok {
						continue
					}
					if _, ok := newElems[name]; ok {
						continue
					}
					newElems[name] = struct{}{}
					count++
				}
				*add.count = count
			}

			rows := make([]store.RSet, 0, len(newElems))
			for elem := range newElems {
				rows = append(rows, store.RSet{KID: rkey.ID, Elem: []byte(elem)})
			}
			if len(rows) > 0 {
				result := store.CreateIgnoreConflict(tx, dialect,
					[]clause.Column{{Name: "kid"}, {Name: "elem"}}, &rows)
				if result.Error != nil {
					return result.Error
				}
			}
			if len(rows) == 0 && !expired {
				return nil
			}

			updates := map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}
			if expired {
				updates["expire_at"] = nil
				updates["klen"] = len(rows)
			} else if len(rows) > 0 {
				updates["klen"] = gorm.Expr("klen + ?", len(rows))
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).Updates(updates).Error
		})

	return newCount, err
}

type setAddWriteItem struct {
	elems [][]byte
	count *int
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
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeSet.Value()).
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

func (d *DB) diffTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	items, err := d.setElemsByKey(db, keys[0])
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []core.Value{}, nil
	}
	elems := setElemSet(items)
	for _, key := range keys[1:] {
		removals, err := d.setElemsByKey(db, key)
		if err != nil {
			return nil, err
		}
		deleteElems(elems, removals)
	}
	return setValues(elems), nil
}

func (d *DB) setElemsByKey(db *gorm.DB, key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()
	var rows []store.RSet
	err := db.Model(&store.RSet{}).
		Joins("JOIN rkey ON rkey.id = rset.kid AND rkey.ktype = ?", core.TypeSet.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	items := make([]core.Value, len(rows))
	for i, row := range rows {
		items[i] = core.Value(row.Elem)
	}
	return items, nil
}

func setValues(elems map[string]struct{}) []core.Value {
	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items
}

// diffTx computes the difference of sets within a transaction.
// Used internally by DiffStore to ensure atomicity.
func (d *DB) diffTxFast(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sets, err := d.setsByKeys(db, keys...)
	if err != nil {
		return nil, err
	}
	return diffSetValues(sets, keys), nil
}

func diffSetValues(sets map[string][]core.Value, keys []string) []core.Value {
	if len(sets[keys[0]]) == 0 {
		return []core.Value{}
	}

	elems := setElemSet(sets[keys[0]])
	for _, key := range keys[1:] {
		deleteElems(elems, sets[key])
	}

	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items
}

type setItemRow struct {
	KeyName string `gorm:"column:key_name"`
	Elem    []byte `gorm:"column:elem"`
}

func (d *DB) setsByKeys(db *gorm.DB, keys ...string) (map[string][]core.Value, error) {
	if len(keys) == 0 {
		return map[string][]core.Value{}, nil
	}

	now := time.Now().UnixMilli()
	var rows []setItemRow
	err := db.Model(&store.RSet{}).
		Select("rkey.kname AS key_name, rset.elem").
		Joins("JOIN rkey ON rkey.id = rset.kid AND rkey.ktype = ?", core.TypeSet.Value()).
		Where("rkey.kdb = ? AND rkey.kname IN ?", d.dbIdx, keys).
		Scopes(store.NotExpired(now)).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}

	sets := make(map[string][]core.Value, len(keys))
	for _, key := range keys {
		sets[key] = nil
	}
	for _, row := range rows {
		sets[row.KeyName] = append(sets[row.KeyName], core.Value(row.Elem))
	}
	return sets, nil
}

func setElemSet(items []core.Value) map[string]struct{} {
	elems := make(map[string]struct{}, len(items))
	for _, item := range items {
		elems[string(item)] = struct{}{}
	}
	return elems
}

func deleteElems(elems map[string]struct{}, removals []core.Value) {
	for _, item := range removals {
		delete(elems, string(item))
	}
}

// DiffStore calculates the difference and stores the result in a destination set.
// The difference calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) DiffStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return d.writeSetStore("diff", dest, keys,
		func(tx *gorm.DB, dialect store.Dialect, keys []string) (int, error) {
			return d.storeSetResult(tx, dialect, dest, func() ([]core.Value, error) {
				return d.diffTxFast(tx, keys...)
			})
		})
}

func (d *DB) writeScope(key string) string {
	return "set:store:" + strconv.Itoa(d.dbIdx) + "\x00" + key
}

func (d *DB) writeSetStore(op, dest string, keys []string,
	compute func(tx *gorm.DB, dialect store.Dialect, keys []string) (int, error)) (int, error) {
	resultCount := 0
	item := setStoreWriteItem{keys: keys, count: &resultCount}
	err := d.store.TransactionWriteItems(context.Background(),
		"set:store:"+op+":"+strconv.Itoa(d.dbIdx)+"\x00"+dest, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			groups := make(map[string][]setStoreWriteItem)
			order := make([]string, 0)
			for _, raw := range items {
				storeItem := raw.(setStoreWriteItem)
				signature := setStoreKeysSignature(storeItem.keys)
				if _, exists := groups[signature]; !exists {
					order = append(order, signature)
				}
				groups[signature] = append(groups[signature], storeItem)
			}

			for _, signature := range order {
				group := groups[signature]
				count, err := compute(tx, dialect, group[len(group)-1].keys)
				if err != nil {
					return err
				}
				for _, storeItem := range group {
					*storeItem.count = count
				}
			}
			return nil
		})
	return resultCount, err
}

func (d *DB) lockOrCreateSetKey(tx *gorm.DB, dialect store.Dialect,
	name string, now int64) (store.RKey, error) {
	key, err := store.LockKey(tx, dialect, d.dbIdx, name)
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return key, err
	}
	return store.EnsureKeyLocked(tx, dialect, d.dbIdx, name,
		core.TypeSet.Value(), now, 0)
}

func setStoreKeysSignature(keys []string) string {
	var signature strings.Builder
	for _, key := range keys {
		signature.WriteString(strconv.Itoa(len(key)))
		signature.WriteByte(':')
		signature.WriteString(key)
		signature.WriteByte('\x1f')
	}
	return signature.String()
}

type setStoreWriteItem struct {
	keys  []string
	count *int
}

func (d *DB) storeSetResult(tx *gorm.DB, dialect store.Dialect, dest string, compute func() ([]core.Value, error)) (int, error) {
	now := time.Now().UnixMilli()
	rkey, err := d.lockOrCreateSetKey(tx, dialect, dest, now)
	if err != nil {
		return 0, err
	}
	if rkey.KType != core.TypeSet.Value() {
		return 0, core.ErrKeyType
	}
	expired := rkey.ExpireAt != nil && *rkey.ExpireAt <= now

	items, err := compute()
	if err != nil {
		return 0, err
	}

	var existing []store.RSet
	if err := tx.Where("kid = ?", rkey.ID).Find(&existing).Error; err != nil {
		return 0, err
	}
	if !expired {
		existingElems := make(map[string]struct{}, len(existing))
		for _, row := range existing {
			existingElems[string(row.Elem)] = struct{}{}
		}
		resultElems := make(map[string]struct{}, len(items))
		for _, item := range items {
			resultElems[string(item)] = struct{}{}
		}
		if len(existingElems) == len(resultElems) {
			unchanged := true
			for elem := range resultElems {
				if _, ok := existingElems[elem]; !ok {
					unchanged = false
					break
				}
			}
			if unchanged {
				return len(items), nil
			}
		}
	}

	if err := tx.Where("kid = ?", rkey.ID).Delete(&store.RSet{}).Error; err != nil {
		return 0, err
	}
	updates := map[string]any{
		"kver":        gorm.Expr("kver + 1"),
		"modified_at": now,
		"klen":        len(items),
	}
	if expired {
		updates["expire_at"] = nil
	}
	if len(items) == 0 {
		return 0, tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(updates).Error
	}

	rows := make([]store.RSet, len(items))
	for i, item := range items {
		rows[i] = store.RSet{KID: rkey.ID, Elem: []byte(item)}
	}
	if err := tx.Create(&rows).Error; err != nil {
		return 0, err
	}
	return len(items), tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(updates).Error
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rset.elem = ?", d.dbIdx, key, elemb).
		Scopes(store.NotExpired(now)).
		Count(&result.Count).Error

	return result.Count > 0, err
}

// Inter returns the intersection of multiple sets.
func (d *DB) Inter(keys ...string) ([]core.Value, error) {
	return d.interTx(d.store.DB, keys...)
}

func (d *DB) interTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	items, err := d.setElemsByKey(db, keys[0])
	if err != nil {
		return nil, err
	}
	if len(items) == 0 {
		return []core.Value{}, nil
	}
	elems := setElemSet(items)
	for _, key := range keys[1:] {
		items, err := d.setElemsByKey(db, key)
		if err != nil {
			return nil, err
		}
		temp := make(map[string]struct{}, len(elems))
		for _, item := range items {
			if _, ok := elems[string(item)]; ok {
				temp[string(item)] = struct{}{}
			}
		}
		elems = temp
		if len(elems) == 0 {
			break
		}
	}
	return setValues(elems), nil
}

// interTx computes the intersection of sets within a transaction.
// Used internally by InterStore to ensure atomicity.
func (d *DB) interTxFast(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sets, err := d.setsByKeys(db, keys...)
	if err != nil {
		return nil, err
	}
	return interSetValues(sets, keys), nil
}

func interSetValues(sets map[string][]core.Value, keys []string) []core.Value {
	if len(sets[keys[0]]) == 0 {
		return []core.Value{}
	}

	elems := setElemSet(sets[keys[0]])
	for _, key := range keys[1:] {
		temp := make(map[string]struct{}, len(elems))
		for _, item := range sets[key] {
			if _, ok := elems[string(item)]; ok {
				temp[string(item)] = struct{}{}
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
	return items
}

// InterStore intersects multiple sets and stores the result in a destination set.
// The intersection calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) InterStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return d.writeSetStore("inter", dest, keys,
		func(tx *gorm.DB, dialect store.Dialect, keys []string) (int, error) {
			return d.storeSetResult(tx, dialect, dest, func() ([]core.Value, error) {
				return d.interTxFast(tx, keys...)
			})
		})
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
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

	// A missing source member is a successful SMOVE result (0), not an error.
	// Checking it before locking both keys avoids hot repeated-move workloads.
	var sourceRow struct {
		ID int `gorm:"column:id"`
	}
	err = d.store.DB.Model(&store.RSet{}).
		Select("rset.kid AS id").
		Joins("JOIN rkey ON rkey.id = rset.kid").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rkey.ktype = ? AND rset.elem = ?",
			d.dbIdx, src, core.TypeSet.Value(), elemb).
		First(&sourceRow).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, core.ErrNotFound
	}
	if err != nil {
		return false, err
	}
	if src == dest {
		return true, nil
	}

	var moved bool
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, dialect store.Dialect) error {
		now := time.Now().UnixMilli()

		// 死锁预防：按键名排序后加锁，确保固定的锁定顺序
		// 这样可以避免两个并发事务以相反顺序锁定相同的键而导致死锁
		keys := []string{src, dest}
		sort.Strings(keys)
		firstKey := keys[0]
		secondKey := keys[1]

		// 按排序后的顺序锁定键
		keyQuery := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, firstKey)
		if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
			keyQuery = keyQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var firstKeyRecord store.RKey
		err := keyQuery.First(&firstKeyRecord).Error

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
			if firstKeyRecord.KType != core.TypeSet.Value() {
				return core.ErrKeyType
			}
			srcKey = firstKeyRecord

			// 检查 dest 是否存在
			destKey, err = store.EnsureKeyLocked(tx, dialect, d.dbIdx, dest,
				core.TypeSet.Value(), now, 0)
			if err != nil {
				return err
			}
			if destKey.KType != core.TypeSet.Value() {
				return core.ErrKeyType
			}
		} else {
			// firstKey 是 dest
			if errors.Is(err, gorm.ErrRecordNotFound) {
				destKey, err = store.EnsureKeyLocked(tx, dialect, d.dbIdx, dest,
					core.TypeSet.Value(), now, 0)
				if err != nil {
					return err
				}
			} else if err != nil {
				return err
			} else {
				if firstKeyRecord.KType != core.TypeSet.Value() {
					return core.ErrKeyType
				}
				destKey = firstKeyRecord
			}

			// 检查 src 是否存在
			srcQuery := tx.Model(&store.RKey{}).
				Where("kdb = ? AND kname = ?", d.dbIdx, secondKey)
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				srcQuery = srcQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			err = srcQuery.First(&srcKey).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}
			if srcKey.KType != core.TypeSet.Value() {
				return core.ErrKeyType
			}
		}

		// 检查源集合中是否存在该元素
		srcElemQuery := tx.Model(&store.RSet{}).
			Where("kid = ? AND elem = ?", srcKey.ID, elemb)
		if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
			srcElemQuery = srcElemQuery.Clauses(clause.Locking{Strength: "UPDATE"})
		}
		var srcSet store.RSet
		err = srcElemQuery.First(&srcSet).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		if src == dest {
			moved = true
			return nil
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
		row := store.RSet{KID: destKey.ID, Elem: elemb}
		if err := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "elem"}}, &row).Error; err != nil {
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
	elem := core.Value{}
	err := error(nil)
	item := setPopWriteItem{elem: &elem, err: &err}
	err = d.store.TransactionWriteItems(context.Background(), d.writeScope(key), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			pops := make([]setPopWriteItem, 0, len(items))
			for _, raw := range items {
				pops = append(pops, raw.(setPopWriteItem))
			}

			var rows []struct {
				ID   int    `gorm:"column:id"`
				Elem []byte `gorm:"column:elem"`
			}
			err := tx.Model(&store.RKey{}).
				Select("rkey.id, rset.elem").
				Joins("JOIN rset ON rset.kid = rkey.id").
				Where("rkey.kdb = ? AND rkey.kname = ? AND rkey.ktype = ?",
					d.dbIdx, key, core.TypeSet.Value()).
				Limit(len(pops)).
				Find(&rows).Error
			if err != nil {
				return err
			}

			for i, pop := range pops {
				if i < len(rows) {
					*pop.elem = core.Value(rows[i].Elem)
					*pop.err = nil
				} else {
					*pop.elem = core.Value{}
					*pop.err = core.ErrNotFound
				}
			}
			if len(rows) == 0 {
				return nil
			}

			keyID := rows[0].ID
			elems := make([][]byte, len(rows))
			for i, row := range rows {
				elems[i] = row.Elem
			}
			if err := tx.Where("kid = ? AND elem IN ?", keyID, elems).
				Delete(&store.RSet{}).Error; err != nil {
				return err
			}

			return tx.Model(&store.RKey{}).
				Where("id = ?", keyID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + ?", len(rows)),
					"modified_at": now,
					"klen":        gorm.Expr("klen - ?", len(rows)),
				}).Error
		})

	return elem, err
}

type setPopWriteItem struct {
	elem *core.Value
	err  *error
}

// Random returns a random element from a set.
func (d *DB) Random(key string) (core.Value, error) {
	now := time.Now().UnixMilli()
	var rset store.RSet
	err := d.store.DB.Model(&store.RSet{}).
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now), store.RandomOrder(d.store.Dialect)).
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
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
		Joins("JOIN rkey ON rset.kid = rkey.id AND rkey.ktype = ?", core.TypeSet.Value()).
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

func (d *DB) unionTx(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	elems := make(map[string]struct{})
	for _, key := range keys {
		items, err := d.setElemsByKey(db, key)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			elems[string(item)] = struct{}{}
		}
	}
	return setValues(elems), nil
}

// unionTx computes the union of sets within a transaction.
// Used internally by UnionStore to ensure atomicity.
func (d *DB) unionTxFast(db *gorm.DB, keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	sets, err := d.setsByKeys(db, keys...)
	if err != nil {
		return nil, err
	}
	return unionSetValues(sets), nil
}

func unionSetValues(sets map[string][]core.Value) []core.Value {
	elems := make(map[string]struct{})
	for _, items := range sets {
		for _, item := range items {
			elems[string(item)] = struct{}{}
		}
	}

	items := make([]core.Value, 0, len(elems))
	for elem := range elems {
		items = append(items, core.Value(elem))
	}
	return items
}

// UnionStore unions multiple sets and stores the result in a destination set.
// The union calculation and storage are performed within the same
// transaction to ensure atomicity.
func (d *DB) UnionStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}
	return d.writeSetStore("union", dest, keys,
		func(tx *gorm.DB, dialect store.Dialect, keys []string) (int, error) {
			return d.storeSetResult(tx, dialect, dest, func() ([]core.Value, error) {
				return d.unionTxFast(tx, keys...)
			})
		})
}
