// Package rset is a database-backed set repository.
// It provides methods to interact with sets in the database.
package rset

import (
	"math/rand"
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

// keyTypeSet is the type ID for set keys.
const keyTypeSet = 3

// DB is a database-backed set repository.
// A set is an unordered collection of unique strings.
// Use the set repository to work with individual sets
// and their elements, and to perform set operations
// like union, intersection.
type DB struct {
	store *store.Store
	dbIdx int
}

// Tx is a set repository transaction.
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	dbIdx   int
}

// Scanner is the iterator for set items.
// Stops when there are no more items or an error occurs.
type Scanner struct {
	tx       *Tx
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
	d := &DB{store: s, dbIdx: 0}
	return d
}

// WithDB changes the logical database index in place and returns the same DB.
// It is safe for concurrent use; each TCP connection has its own DB instance.
func (d *DB) WithDB(dbIdx int) *DB {
	newDB := *d
	newDB.dbIdx = dbIdx
	return &newDB
}

// NewTx creates a set repository transaction
// from a generic database transaction.
func NewTx(dialect store.Dialect, tx *gorm.DB, dbIdx int) *Tx {
	return &Tx{dialect: dialect, tx: tx, dbIdx: dbIdx}
}

// Add adds or updates elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elems ...any) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Add(key, elems...)
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Delete(key, elems...)
}

// Diff returns the difference between the first set and the rest.
// The difference consists of elements that are present in the first set
// but not in any of the rest.
// If the first key does not exist or is not a set, returns an empty slice.
// If any of the remaining keys do not exist or are not sets, ignores them.
func (d *DB) Diff(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Diff(keys...)
}

// DiffStore calculates the difference between the first source set
// and the rest, and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If the first source key does not exist or is not a set, does nothing,
// except deleting the destination key if it exists.
// If any of the remaining source keys do not exist or are not sets, ignores them.
func (d *DB) DiffStore(dest string, keys ...string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.DiffStore(dest, keys...)
}

// Exists reports whether the element belongs to a set.
// If the key does not exist or is not a set, returns false.
func (d *DB) Exists(key, elem any) (bool, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Exists(key, elem)
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// If any of the source keys do not exist or are not sets,
// returns an empty slice.
func (d *DB) Inter(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Inter(keys...)
}

// InterStore intersects multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (d *DB) InterStore(dest string, keys ...string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.InterStore(dest, keys...)
}

// InterCard returns the number of elements in the intersection of multiple sets.
// InterCard returns the number of elements in the intersection of multiple sets.
// If limit > 0, the command stops counting once the count reaches the limit.
// If any source key does not exist or is not a set, returns 0.
func (d *DB) InterCard(limit int, keys ...string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.InterCard(limit, keys...)
}

// ExistsMany checks if multiple members exist in a set.
// Returns a slice of booleans indicating existence for each member.
// If the key does not exist or is not a set, returns all false.
func (d *DB) ExistsMany(key string, members ...any) ([]bool, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.ExistsMany(key, members...)
}

// Items returns all elements in a set.
// If the key does not exist or is not a set, returns an empty slice.
func (d *DB) Items(key string) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Items(key)
}

// Len returns the number of elements in a set.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Len(key)
}

// Move moves an element from one set to another.
// If the element does not exist in the source set, returns ErrNotFound.
// If the source key does not exist or is not a set, returns ErrNotFound.
// If the destination key does not exist, creates it.
// If the destination key exists but is not a set, returns ErrKeyType.
// If the element already exists in the destination set,
// only deletes it from the source set.
func (d *DB) Move(src, dest string, elem any) (bool, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Move(src, dest, elem)
}

// Pop removes and returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) Pop(key string) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Pop(key)
}

// Random returns a random element from a set.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) Random(key string) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Random(key)
}

// RandMember returns multiple random elements from a set.
// If count is positive, returns unique elements.
// If count is negative, allows duplicates.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) RandMember(key string, count int) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.RandMember(key, count)
}

// Scan iterates over set elements matching pattern.
// Returns a slice of elements of size count based on the current state
// of the cursor. Returns an empty slice when there are no more items.
// If the key does not exist or is not a set, returns an empty slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator over set elements matching pattern.
// The scanner returns items one by one, fetching them from the database
// in pageSize batches when necessary. Stops when there are no more items
// or an error occurs. If the key does not exist or is not a set, stops immediately.
// Supports glob-style patterns. Set pageSize = 0 for default page size.
func (d *DB) Scanner(key, pattern string, pageSize int) *Scanner {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Scanner(key, pattern, pageSize)
}

// Union returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns an empty slice.
func (d *DB) Union(keys ...string) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Union(keys...)
}

// UnionStore unions multiple sets and stores the result in a destination set.
// Returns the number of elements in the destination set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// If all of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (d *DB) UnionStore(dest string, keys ...string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.UnionStore(dest, keys...)
}

// Tx methods

// Add adds or updates elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
func (tx *Tx) Add(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	now := time.Now().UnixMilli()
	newCount := 0

	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create the key with type checking
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		switch err {
		case nil:
			// Key exists, check type
			if rkey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			// Update version and mtime
			if err := txInner.Model(&rkey).Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&rkey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Add elements with ON CONFLICT DO NOTHING
		for _, elemb := range elembs {
			rset := store.RSet{KID: rkey.ID, Elem: elemb}
			result := txInner.Clauses(clause.OnConflict{DoNothing: true}).
				Create(&rset)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected > 0 {
				newCount++
			}
		}

		// Update key metadata if elements were added
		if newCount > 0 {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Update("klen", gorm.Expr("klen + ?", newCount)).Error
		}

		return nil
	})

	return newCount, err
}

// AreEqual checks if two sets are equal.
func (tx *Tx) AreEqual(key1, key2 string) (bool, error) {
	now := time.Now().UnixMilli()

	// Get kids for both keys
	var kid1, kid2 int64
	err1 := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key1, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&kid1).Error
	err2 := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key2, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&kid2).Error

	if err1 == gorm.ErrRecordNotFound || err2 == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err1 != nil {
		return false, err1
	}
	if err2 != nil {
		return false, err2
	}

	// Check if sets are equal by counting elements that exist in only one set
	var count int64
	err := tx.tx.Model(&store.RSet{}).
		Where("kid IN ?", []int64{kid1, kid2}).
		Group("elem").
		Having("COUNT(DISTINCT kid) = 1").
		Count(&count).Error

	return count == 0, err
}

// Delete removes elements from a set.
// Returns the number of elements deleted.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	now := time.Now().UnixMilli()

	// Get kid for the key
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Delete elements
	result := tx.tx.Where("kid = ? AND elem IN ?", rkey.ID, elembs).
		Delete(&store.RSet{})
	if result.Error != nil {
		return 0, result.Error
	}

	n := int(result.RowsAffected)
	if n > 0 {
		// Update key metadata
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", n),
			})
	}

	return n, nil
}

// Items returns all elements in a set.
func (tx *Tx) Items(key string) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return []core.Value{}, nil
	}
	if err != nil {
		return nil, err
	}

	var rsets []store.RSet
	err = tx.tx.Where("kid = ?", rkey.ID).Find(&rsets).Error
	if err != nil {
		return nil, err
	}

	items := make([]core.Value, len(rsets))
	for i, rset := range rsets {
		items[i] = core.Value(rset.Elem)
	}
	return items, nil
}

// Len returns the number of elements in a set.
func (tx *Tx) Len(key string) (int, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
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

// Pop removes and returns a random element from a set.
func (tx *Tx) Pop(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	var elem []byte
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get key
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if err == gorm.ErrRecordNotFound {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		// Get random element
		var rset store.RSet
		err = txInner.Where("kid = ?", rkey.ID).
			Scopes(store.RandomOrder(tx.dialect)).
			First(&rset).Error
		if err == gorm.ErrRecordNotFound {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		elem = rset.Elem

		// Delete the element
		result := txInner.Where("kid = ? AND elem = ?", rkey.ID, elem).
			Delete(&store.RSet{})
		if result.Error != nil {
			return result.Error
		}

		// Update key
		return txInner.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			}).Error
	})

	return core.Value(elem), err
}

// Scan iterates over set elements matching pattern.
// Returns a cursor for the next page and the items.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count <= 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()

	// Get kid for the key
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return ScanResult{}, nil
	}
	if err != nil {
		return ScanResult{}, err
	}

	// Build query with pattern matching
	var rsets []store.RSet
	if tx.dialect == store.DialectSQLite {
		err = tx.tx.Where("kid = ? AND id > ?", rkey.ID, cursor).
			Where("elem GLOB ?", pattern).
			Order("id ASC").
			Limit(count).
			Find(&rsets).Error
	} else {
		err = tx.tx.Where("kid = ? AND id > ?", rkey.ID, cursor).
			Where(store.ElemPattern(tx.dialect, "elem", pattern)).
			Order("id ASC").
			Limit(count).
			Find(&rsets).Error
	}
	if err != nil {
		return ScanResult{}, err
	}

	items := make([]core.Value, len(rsets))
	var nextCursor int
	for i, rset := range rsets {
		items[i] = core.Value(rset.Elem)
		nextCursor = rset.ID
	}

	if nextCursor == 0 {
		return ScanResult{}, nil
	}
	return ScanResult{Cursor: nextCursor, Items: items}, nil
}

// Diff returns the difference between the first set and the rest.
func (tx *Tx) Diff(keys ...string) ([]core.Value, error) {
	if len(keys) < 2 {
		return []core.Value{}, nil
	}

	now := time.Now().UnixMilli()
	firstKey := keys[0]
	otherKeys := keys[1:]

	// Get kid for first key
	var firstKid int64
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, firstKey, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&firstKid).Error
	if err == gorm.ErrRecordNotFound {
		return []core.Value{}, nil
	}
	if err != nil {
		return nil, err
	}

	// Get kids for other keys
	var otherKids []int64
	err = tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, otherKeys, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&otherKids).Error
	if err != nil {
		return nil, err
	}

	// If no other keys exist, return all elements from first set
	if len(otherKids) == 0 {
		return tx.Items(firstKey)
	}

	// Build subquery for elements in other sets
	subQuery := tx.tx.Model(&store.RSet{}).
		Select("elem").
		Where("kid IN ?", otherKids)

	// Query elements in first set that are not in other sets
	var rsets []store.RSet
	err = tx.tx.Where("kid = ? AND elem NOT IN (?)", firstKid, subQuery).
		Find(&rsets).Error
	if err != nil {
		return nil, err
	}

	result := make([]core.Value, len(rsets))
	for i, rset := range rsets {
		result[i] = core.Value(rset.Elem)
	}
	return result, nil
}

// DiffStore calculates the difference between the first source set and the rest.
func (tx *Tx) DiffStore(dest string, keys ...string) (int, error) {
	if len(keys) < 2 {
		return 0, nil
	}

	now := time.Now().UnixMilli()
	firstKey := keys[0]
	otherKeys := keys[1:]

	var n int64
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create destination key with type checking
		var destKey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			// Delete existing elements in destination
			if err := txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{}).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new key
			destKey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      dest,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&destKey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Get kid for first key
		var firstKid int64
		err = txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, firstKey, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Select("id").
			Scan(&firstKid).Error
		if err == gorm.ErrRecordNotFound {
			return nil // Source doesn't exist, nothing to store
		}
		if err != nil {
			return err
		}

		// Get kids for other keys
		var otherKids []int64
		err = txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, otherKeys, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Select("id").
			Scan(&otherKids).Error
		if err != nil {
			return err
		}

		// Get diff elements
		var diffElems []store.RSet
		if len(otherKids) == 0 {
			// No other keys exist, get all elements from first set
			err = txInner.Model(&store.RSet{}).
				Select("elem").
				Where("kid = ?", firstKid).
				Scan(&diffElems).Error
		} else {
			subQuery := txInner.Model(&store.RSet{}).
				Select("elem").
				Where("kid IN ?", otherKids)
			err = txInner.Model(&store.RSet{}).
				Select("elem").
				Where("kid = ? AND elem NOT IN (?)", firstKid, subQuery).
				Scan(&diffElems).Error
		}
		if err != nil {
			return err
		}

		if len(diffElems) == 0 {
			// No elements to store, delete destination key
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{})
			txInner.Delete(&destKey)
			n = 0
			return nil
		}

		// Prepare elements for insertion
		newRsets := make([]store.RSet, len(diffElems))
		for i, e := range diffElems {
			newRsets[i] = store.RSet{KID: destKey.ID, Elem: e.Elem}
		}

		// Insert elements
		err = txInner.CreateInBatches(newRsets, 100).Error
		if err != nil {
			return err
		}
		n = int64(len(newRsets))

		// Update destination key length
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        n,
			}).Error
	})

	return int(n), err
}

// Exists reports whether the element belongs to a set.
func (tx *Tx) Exists(key, elem any) (bool, error) {
	keyStr, ok := key.(string)
	if !ok {
		return false, core.ErrValueType
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()

	// Get the key
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, keyStr, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	// Check if element exists in the set
	var count int64
	err = tx.tx.Model(&store.RSet{}).
		Where("kid = ? AND elem = ?", rkey.ID, elemb).
		Count(&count).Error

	return count > 0, err
}

// Inter returns the intersection of multiple sets.
func (tx *Tx) Inter(keys ...string) ([]core.Value, error) {
	if len(keys) < 2 {
		return []core.Value{}, nil
	}

	now := time.Now().UnixMilli()

	// Get kids for all keys
	var kids []int64
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, keys, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&kids).Error
	if err != nil {
		return nil, err
	}

	// Need all keys to exist
	if len(kids) < len(keys) {
		return []core.Value{}, nil
	}

	// Find elements that exist in all sets
	type elemResult struct {
		Elem []byte
	}
	var results []elemResult
	err = tx.tx.Model(&store.RSet{}).
		Select("elem").
		Where("kid IN ?", kids).
		Group("elem").
		Having("COUNT(DISTINCT kid) = ?", len(keys)).
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	// Convert to []core.Value
	result := make([]core.Value, len(results))
	for i, r := range results {
		result[i] = core.Value(r.Elem)
	}
	return result, nil
}

// InterStore intersects multiple sets and stores the result.
func (tx *Tx) InterStore(dest string, keys ...string) (int, error) {
	if len(keys) < 2 {
		return 0, nil
	}

	now := time.Now().UnixMilli()
	var n int64
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create destination key with type checking
		var destKey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			// Delete existing elements
			if err := txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{}).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new key
			destKey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      dest,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&destKey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Get kids for all keys
		var kids []int64
		err = txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, keys, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Select("id").
			Scan(&kids).Error
		if err != nil {
			return err
		}

		// Need all keys to exist
		if len(kids) < len(keys) {
			// Delete destination key if no intersection
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{})
			txInner.Delete(&destKey)
			n = 0
			return nil
		}

		// Get intersection elements
		type elemResult struct {
			Elem []byte
		}
		var interElems []elemResult
		err = txInner.Model(&store.RSet{}).
			Select("elem").
			Where("kid IN ?", kids).
			Group("elem").
			Having("COUNT(DISTINCT kid) = ?", len(keys)).
			Scan(&interElems).Error
		if err != nil {
			return err
		}

		if len(interElems) == 0 {
			// No elements to store, delete destination key
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{})
			txInner.Delete(&destKey)
			n = 0
			return nil
		}

		// Prepare elements for insertion
		newRsets := make([]store.RSet, len(interElems))
		for i, e := range interElems {
			newRsets[i] = store.RSet{KID: destKey.ID, Elem: e.Elem}
		}

		// Insert elements
		err = txInner.CreateInBatches(newRsets, 100).Error
		if err != nil {
			return err
		}
		n = int64(len(newRsets))

		// Update destination key length
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        n,
			}).Error
	})

	return int(n), err
}

// InterCard returns the number of elements in the intersection of multiple sets.
// If limit > 0, stops counting once limit is reached.
func (tx *Tx) InterCard(limit int, keys ...string) (int, error) {
	if len(keys) < 2 {
		return 0, nil
	}

	now := time.Now().UnixMilli()

	// Get kids for all keys
	var kids []int64
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, keys, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&kids).Error
	if err != nil {
		return 0, err
	}

	// Need all keys to exist
	if len(kids) < len(keys) {
		return 0, nil
	}

	if limit > 0 {
		// With limit, fetch elements and count until limit
		type elemResult struct {
			Elem []byte
		}
		var results []elemResult
		err = tx.tx.Model(&store.RSet{}).
			Select("elem").
			Where("kid IN ?", kids).
			Group("elem").
			Having("COUNT(DISTINCT kid) = ?", len(keys)).
			Limit(limit).
			Scan(&results).Error
		if err != nil {
			return 0, err
		}
		return len(results), nil
	}

	// Without limit, count all intersection elements
	// Use a subquery to correctly count the intersection cardinality
	type countResult struct {
		Count int
	}
	var result countResult

	err = tx.tx.Raw(`
		SELECT COUNT(*) as count FROM (
			SELECT elem FROM rset
			WHERE kid IN ?
			GROUP BY elem
			HAVING COUNT(DISTINCT kid) = ?
		) AS intersection
	`, kids, len(keys)).Scan(&result).Error
	if err != nil {
		return 0, err
	}
	return result.Count, nil
}

// ExistsMany checks if multiple members exist in a set.
// Returns a slice of booleans for each member.
func (tx *Tx) ExistsMany(key string, members ...any) ([]bool, error) {
	if len(members) == 0 {
		return []bool{}, nil
	}

	now := time.Now().UnixMilli()

	// Get the key
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		// Key doesn't exist, return all false
		result := make([]bool, len(members))
		return result, nil
	}
	if err != nil {
		return nil, err
	}

	// Convert members to bytes
	elems := make([][]byte, len(members))
	for i, m := range members {
		elemb, err := core.ToBytes(m)
		if err != nil {
			return nil, err
		}
		elems[i] = elemb
	}

	// Batch check existence
	type existResult struct {
		Elem  []byte
		Count int64
	}
	var results []existResult
	err = tx.tx.Model(&store.RSet{}).
		Select("elem, COUNT(*) as count").
		Where("kid = ? AND elem IN ?", rkey.ID, elems).
		Group("elem").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	// Build existence map
	existMap := make(map[string]bool, len(results))
	for _, r := range results {
		existMap[string(r.Elem)] = true
	}

	// Build result
	result := make([]bool, len(members))
	for i, elem := range elems {
		result[i] = existMap[string(elem)]
	}
	return result, nil
}

// Move removes an element from one set and adds it to another.
// Returns true if the element was moved, false otherwise.
func (tx *Tx) Move(src, dst string, elem any) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	now := time.Now().UnixMilli()
	var moved bool
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get source key
		var srcKey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, src, keyTypeSet).
			Scopes(store.NotExpired(now)).
			First(&srcKey).Error
		if err == gorm.ErrRecordNotFound {
			return nil // Source doesn't exist
		}
		if err != nil {
			return err
		}

		// Check if element exists in source
		var count int64
		err = txInner.Model(&store.RSet{}).
			Where("kid = ? AND elem = ?", srcKey.ID, elemb).
			Count(&count).Error
		if err != nil {
			return err
		}
		if count == 0 {
			return nil // Element doesn't exist in source
		}

		// Get or create destination key
		var destKey store.RKey
		err = txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, dst).
			Scopes(store.NotExpired(now)).
			First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
		case gorm.ErrRecordNotFound:
			// Create new destination key
			destKey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      dst,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&destKey).Error; err != nil {
				return tx.dialect.TypedError(err)
			}
		default:
			return err
		}

		// Delete from source
		result := txInner.Where("kid = ? AND elem = ?", srcKey.ID, elemb).
			Delete(&store.RSet{})
		if result.Error != nil {
			return result.Error
		}

		// Update source key length
		txInner.Model(&store.RKey{}).
			Where("id = ?", srcKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			})

		// Insert into destination (if not exists)
		rset := store.RSet{KID: destKey.ID, Elem: elemb}

		// Check if element already exists in destination
		var destCount int64
		err = txInner.Model(&store.RSet{}).
			Where("kid = ? AND elem = ?", destKey.ID, elemb).
			Count(&destCount).Error
		if err != nil {
			return err
		}

		if err := txInner.Clauses(clause.OnConflict{DoNothing: true}).Create(&rset).Error; err != nil {
			return err
		}

		// Update destination key length only if element is new
		if destCount == 0 {
			txInner.Model(&store.RKey{}).
				Where("id = ?", destKey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen + 1"),
				})
		}

		moved = true
		return nil
	})

	return moved, err
}

// Random returns a random element from a set.
func (tx *Tx) Random(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	// Get key
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Get random element
	var rset store.RSet
	err = tx.tx.Where("kid = ?", rkey.ID).
		Scopes(store.RandomOrder(tx.dialect)).
		First(&rset).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return core.Value(rset.Elem), nil
}

// RandMember returns multiple random elements from a set.
func (tx *Tx) RandMember(key string, count int) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", tx.dbIdx, key, keyTypeSet).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	var rset []store.RSet
	err = tx.tx.Where("kid = ?", rkey.ID).
		Scopes(store.RandomOrder(tx.dialect)).
		Find(&rset).Error
	if err != nil {
		return nil, err
	}

	if len(rset) == 0 {
		return nil, core.ErrNotFound
	}

	elems := make([]core.Value, 0, len(rset))
	for _, v := range rset {
		elems = append(elems, core.Value(v.Elem))
	}

	if count > 0 && count < len(elems) {
		elems = elems[:count]
	} else if count < 0 && -count < len(elems) {
		result := make([]core.Value, -count)
		for i := 0; i < -count; i++ {
			result[i] = elems[rand.Intn(len(elems))]
		}
		return result, nil
	}

	return elems, nil
}

// Scanner returns an iterator for set items with elements matching pattern.
func (tx *Tx) Scanner(key string, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Union returns the union of multiple sets.
func (tx *Tx) Union(keys ...string) ([]core.Value, error) {
	if len(keys) == 0 {
		return []core.Value{}, nil
	}

	now := time.Now().UnixMilli()

	// Get kids for all keys
	var kids []int64
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, keys, keyTypeSet).
		Scopes(store.NotExpired(now)).
		Select("id").
		Scan(&kids).Error
	if err != nil {
		return nil, err
	}

	if len(kids) == 0 {
		return []core.Value{}, nil
	}

	// Find distinct elements
	type elemResult struct {
		Elem []byte
	}
	var results []elemResult
	err = tx.tx.Model(&store.RSet{}).
		Distinct("elem").
		Where("kid IN ?", kids).
		Order("elem").
		Scan(&results).Error
	if err != nil {
		return nil, err
	}

	// Convert to []core.Value
	result := make([]core.Value, len(results))
	for i, r := range results {
		result[i] = core.Value(r.Elem)
	}
	return result, nil
}

// UnionStore unions multiple sets and stores the result.
func (tx *Tx) UnionStore(dest string, keys ...string) (int, error) {
	if len(keys) == 0 {
		return 0, nil
	}

	now := time.Now().UnixMilli()
	var n int64
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create destination key with type checking
		var destKey store.RKey
		err := txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", tx.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != keyTypeSet {
				return core.ErrKeyType
			}
			// Delete existing elements
			if err := txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{}).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Create new key
			destKey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      dest,
				KType:      keyTypeSet,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&destKey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Get kids for all keys
		var kids []int64
		err = txInner.Model(&store.RKey{}).
			Where("kdb = ? AND kname IN ? AND ktype = ?", tx.dbIdx, keys, keyTypeSet).
			Scopes(store.NotExpired(now)).
			Select("id").
			Scan(&kids).Error
		if err != nil {
			return err
		}

		if len(kids) == 0 {
			// No source keys exist, delete destination key
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{})
			txInner.Delete(&destKey)
			n = 0
			return nil
		}

		// Get union elements
		type elemResult struct {
			Elem []byte
		}
		var unionElems []elemResult
		err = txInner.Model(&store.RSet{}).
			Distinct("elem").
			Where("kid IN ?", kids).
			Scan(&unionElems).Error
		if err != nil {
			return err
		}

		if len(unionElems) == 0 {
			// No elements to store, delete destination key
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RSet{})
			txInner.Delete(&destKey)
			n = 0
			return nil
		}

		// Prepare elements for insertion
		newRsets := make([]store.RSet, len(unionElems))
		for i, e := range unionElems {
			newRsets[i] = store.RSet{KID: destKey.ID, Elem: e.Elem}
		}

		// Insert elements
		err = txInner.CreateInBatches(newRsets, 100).Error
		if err != nil {
			return err
		}
		n = int64(len(newRsets))

		// Update destination key length
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        n,
			}).Error
	})

	return int(n), err
}

// Scanner methods

func newScanner(tx *Tx, key string, pattern string, pageSize int) *Scanner {
	if pageSize == 0 {
		pageSize = scanPageSize
	}
	return &Scanner{
		tx:       tx,
		key:      key,
		cursor:   0,
		pattern:  pattern,
		pageSize: pageSize,
		index:    0,
		items:    []core.Value{},
	}
}

// Scan advances to the next item, fetching items from db as necessary.
// Returns false when there are no more items or an error occurs.
// Returns false if the key does not exist or is not a set.
func (sc *Scanner) Scan() bool {
	if sc.index >= len(sc.items) {
		// Fetch a new page of items.
		result, err := sc.tx.Scan(sc.key, sc.cursor, sc.pattern, sc.pageSize)
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

// Item returns the current set item.
func (sc *Scanner) Item() core.Value {
	return sc.cur
}

// Err returns the first error encountered during iteration.
func (sc *Scanner) Err() error {
	return sc.err
}
