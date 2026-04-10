// Package rlist is a database-backed list repository.
// It provides methods to interact with lists in the database.
package rlist

import (
	"context"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
)

// DB is a database-backed list repository.
// A list is a sequence of strings ordered by insertion order.
// Use the list repository to work with lists and their elements.
type DB struct {
	store  *store.Store
	update func(f func(tx *Tx) error) error
	dbIdx  int
}

// New connects to the list repository.
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

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) Delete(key string, elem any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, elem)
		return err
	})
	return n, err
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) DeleteBack(key string, elem any, count int) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.DeleteBack(key, elem, count)
		return err
	})
	return n, err
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (d *DB) DeleteFront(key string, elem any, count int) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.DeleteFront(key, elem, count)
		return err
	})
	return n, err
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) Get(key string, idx int) (core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Get(key, idx)
}

// InsertAfter inserts an element after another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (d *DB) InsertAfter(key string, pivot, elem any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.InsertAfter(key, pivot, elem)
		return err
	})
	return n, err
}

// InsertBefore inserts an element before another element (pivot).
// Returns the length of the list after the operation.
// If the pivot does not exist, returns (-1, ErrNotFound).
// If the key does not exist or is not a list, returns (0, ErrNotFound).
func (d *DB) InsertBefore(key string, pivot, elem any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.InsertBefore(key, pivot, elem)
		return err
	})
	return n, err
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Len(key)
}

// PopBack removes and returns the last element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopBack(key string) (core.Value, error) {
	var elem core.Value
	err := d.update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopBack(key)
		return err
	})
	return elem, err
}

// PopBackPushFront removes the last element of a list
// and prepends it to another list (or the same list).
// If the source key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopBackPushFront(src, dest string) (core.Value, error) {
	var elem core.Value
	err := d.update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopBackPushFront(src, dest)
		return err
	})
	return elem, err
}

// PopFront removes and returns the first element of a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) PopFront(key string) (core.Value, error) {
	var elem core.Value
	err := d.update(func(tx *Tx) error {
		var err error
		elem, err = tx.PopFront(key)
		return err
	})
	return elem, err
}

// PushBack appends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (d *DB) PushBack(key string, elem any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.PushBack(key, elem)
		return err
	})
	return n, err
}

// PushFront prepends an element to a list.
// Returns the length of the list after the operation.
// If the key does not exist, creates it.
// If the key exists but is not a list, returns ErrKeyType.
func (d *DB) PushFront(key string, elem any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.PushFront(key, elem)
		return err
	})
	return n, err
}

// Range returns a range of elements from a list.
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the key does not exist or is not a list, returns an empty slice.
func (d *DB) Range(key string, start, stop int) ([]core.Value, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Range(key, start, stop)
}

// Set sets an element in a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (d *DB) Set(key string, idx int, elem any) error {
	err := d.update(func(tx *Tx) error {
		return tx.Set(key, idx, elem)
	})
	return err
}

// Trim removes elements from both ends of a list so that
// only the elements between start and stop indexes remain.
// Returns the number of elements removed.
//
// Both start and stop are zero-based, inclusive.
// Negative indexes count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
//
// Does nothing if the key does not exist or is not a list.
func (d *DB) Trim(key string, start, stop int) error {
	return d.update(func(tx *Tx) error {
		return tx.Trim(key, start, stop)
	})
}

// Tx is a list repository transaction.
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	dbIdx   int
}

// NewTx creates a list repository transaction
// from a generic database transaction.
func NewTx(dialect store.Dialect, tx *gorm.DB, dbIdx int) *Tx {
	return &Tx{dialect: dialect, tx: tx, dbIdx: dbIdx}
}

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) Delete(key string, elem any) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	// Get the key id first
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Select("id").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Delete all matching elements
	result := tx.tx.Where("kid = ? AND elem = ?", rkey.ID, elemb).Delete(&store.RList{})
	if result.Error != nil {
		return 0, result.Error
	}

	// Update key metadata
	if result.RowsAffected > 0 {
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", result.RowsAffected),
			})
	}

	return int(result.RowsAffected), nil
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteBack(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, true)
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
func (tx *Tx) DeleteFront(key string, elem any, count int) (int, error) {
	return tx.delete(key, elem, count, false)
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
// If the index is out of bounds, returns ErrNotFound.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) Get(key string, idx int) (core.Value, error) {
	now := time.Now().UnixMilli()

	// Get the key id first
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Calculate actual index
	actualIdx := idx
	if idx < 0 {
		actualIdx = rkey.KLen + idx
	}
	if actualIdx < 0 || actualIdx >= rkey.KLen {
		return nil, core.ErrNotFound
	}

	// Get element at position (always use ASC order with correct offset)
	var rlist store.RList
	err = tx.tx.Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Offset(actualIdx).
		Limit(1).
		First(&rlist).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return core.Value(rlist.Elem), nil
}

// InsertAfter inserts an element after another element.
// Returns the new length of the list.
func (tx *Tx) InsertAfter(key string, pivot any, elem any) (int, error) {
	return tx.insert(key, pivot, elem, true)
}

// InsertBefore inserts an element before another element.
// Returns the new length of the list.
func (tx *Tx) InsertBefore(key string, pivot any, elem any) (int, error) {
	return tx.insert(key, pivot, elem, false)
}

// Len returns the number of elements in a list.
// If the key does not exist or is not a list, returns 0.
func (tx *Tx) Len(key string) (int, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
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

// PopBack removes and returns the last element from a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBack(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	// Get the key id first
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if rkey.KLen == 0 {
		return nil, core.ErrNotFound
	}

	// Get and delete the last element
	var rlist store.RList
	err = tx.tx.Where("kid = ?", rkey.ID).
		Order("pos DESC").
		Limit(1).
		First(&rlist).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Delete the element
	result := tx.tx.Where("id = ?", rlist.ID).Delete(&store.RList{})
	if result.Error != nil {
		return nil, result.Error
	}

	// Update key metadata
	tx.tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        gorm.Expr("klen - 1"),
		})

	return core.Value(rlist.Elem), nil
}

// PopFront removes and returns the first element from a list.
// If the key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopFront(key string) (core.Value, error) {
	now := time.Now().UnixMilli()

	// Get the key id first
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	if rkey.KLen == 0 {
		return nil, core.ErrNotFound
	}

	// Get and delete the first element
	var rlist store.RList
	err = tx.tx.Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Limit(1).
		First(&rlist).Error
	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	// Delete the element
	result := tx.tx.Where("id = ?", rlist.ID).Delete(&store.RList{})
	if result.Error != nil {
		return nil, result.Error
	}

	// Update key metadata
	tx.tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        gorm.Expr("klen - 1"),
		})

	return core.Value(rlist.Elem), nil
}

// PopBackPushFront removes the last element from src list
// and prepends it to dest list. Returns the popped element.
// If the source key does not exist or is not a list, returns ErrNotFound.
func (tx *Tx) PopBackPushFront(src, dest string) (core.Value, error) {
	now := time.Now().UnixMilli()

	var elem []byte
	err := tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get src key
		var srcKey store.RKey
		if err := txInner.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, src).
			Scopes(store.NotExpired(now)).
			First(&srcKey).Error; err != nil {
			if err == gorm.ErrRecordNotFound {
				return core.ErrNotFound
			}
			return err
		}

		if srcKey.KLen == 0 {
			return core.ErrNotFound
		}

		// Get the last element from src
		var srcElem store.RList
		if err := txInner.Where("kid = ?", srcKey.ID).
			Order("pos DESC").
			Limit(1).
			First(&srcElem).Error; err != nil {
			return err
		}
		elem = srcElem.Elem

		// Delete the element from src
		if err := txInner.Where("id = ?", srcElem.ID).Delete(&store.RList{}).Error; err != nil {
			return err
		}

		// Update src key metadata
		txInner.Model(&store.RKey{}).
			Where("id = ?", srcKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			})

		// Check if dest key exists and validate type
		var destKey store.RKey
		err := txInner.Model(&store.RKey{}).
			Select("id", "ktype", "klen").
			Where("kdb = ? AND kname = ?", tx.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			First(&destKey).Error

		switch err {
		case nil:
			// Dest key exists, check type
			if destKey.KType != 2 {
				return core.ErrKeyType
			}
		case gorm.ErrRecordNotFound:
			// Dest key does not exist, create new key
			destKey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      dest,
				KType:      2,
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

		// Calculate new position for dest (front)
		var minPos *int64
		txInner.Model(&store.RList{}).
			Select("MIN(pos)").
			Where("kid = ?", destKey.ID).
			Scan(&minPos)

		newPos := int64(0)
		if minPos != nil {
			newPos = *minPos - 1
		}

		// Insert element at front of dest
		if err := txInner.Create(&store.RList{
			KID:  destKey.ID,
			Pos:  newPos,
			Elem: elem,
		}).Error; err != nil {
			return err
		}

		// Update dest key metadata: increment version, mtime, and len
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen + 1"),
			}).Error
	})

	if err == gorm.ErrRecordNotFound {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

// PushBack appends an element to the back of a list.
// Returns the new length of the list.
func (tx *Tx) PushBack(key string, elem any) (int, error) {
	return tx.push(key, elem, true)
}

// PushFront prepends an element to the front of a list.
// Returns the new length of the list.
func (tx *Tx) PushFront(key string, elem any) (int, error) {
	return tx.push(key, elem, false)
}

// Range returns a slice of a list.
// Start and stop are 0-based indexes.
// Negative indexes count from the end of the list.
// Out of bounds indexes are clamped to the nearest valid value.
func (tx *Tx) Range(key string, start, stop int) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	// Get the key id and length
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return []core.Value{}, nil
	}
	if err != nil {
		return nil, err
	}

	// Calculate bounds
	actualStart := start
	actualStop := stop
	if start < 0 {
		actualStart = rkey.KLen + start
	}
	if stop < 0 {
		actualStop = rkey.KLen + stop
	}
	if actualStart < 0 {
		actualStart = 0
	}
	if actualStop >= rkey.KLen {
		actualStop = rkey.KLen - 1
	}
	if actualStart > actualStop {
		return []core.Value{}, nil
	}

	count := actualStop - actualStart + 1

	// Get elements in range
	var rlists []store.RList
	err = tx.tx.Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Offset(actualStart).
		Limit(count).
		Find(&rlists).Error
	if err != nil {
		return nil, err
	}

	values := make([]core.Value, len(rlists))
	for i, rl := range rlists {
		values[i] = core.Value(rl.Elem)
	}
	return values, nil
}

// Set sets the element at index in a list.
// Returns ErrNotFound if the index is out of bounds.
func (tx *Tx) Set(key string, idx int, elem any) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()

	// Get the key id and length
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return core.ErrNotFound
	}
	if err != nil {
		return err
	}

	// Calculate actual index
	actualIdx := idx
	if idx < 0 {
		actualIdx = rkey.KLen + idx
	}
	if actualIdx < 0 || actualIdx >= rkey.KLen {
		return core.ErrNotFound
	}

	// Get element at position to find its pos value (always use ASC order)
	var targetElem store.RList
	err = tx.tx.Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Offset(actualIdx).
		Limit(1).
		First(&targetElem).Error
	if err == gorm.ErrRecordNotFound {
		return core.ErrNotFound
	}
	if err != nil {
		return err
	}

	// Update the element
	result := tx.tx.Model(&store.RList{}).
		Where("id = ?", targetElem.ID).
		Update("elem", elemb)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return core.ErrNotFound
	}

	// Update key metadata
	tx.tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
		})

	return nil
}

// Trim trims a list so that it contains only the specified range of elements.
// Start and stop are 0-based indexes.
// Negative indexes count from the end of the list.
func (tx *Tx) Trim(key string, start, stop int) error {
	now := time.Now().UnixMilli()

	// Get the key id and length
	var rkey store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return nil
	}
	if err != nil {
		return err
	}

	// Calculate bounds
	actualStart := start
	actualStop := stop
	if start < 0 {
		actualStart = rkey.KLen + start
	}
	if stop < 0 {
		actualStop = rkey.KLen + stop
	}
	if actualStart < 0 {
		actualStart = 0
	}
	if actualStop >= rkey.KLen {
		actualStop = rkey.KLen - 1
	}
	if actualStart > actualStop {
		// Delete all elements
		result := tx.tx.Where("kid = ?", rkey.ID).Delete(&store.RList{})
		if result.Error != nil {
			return result.Error
		}
		// Update key metadata
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        0,
			})
		return nil
	}

	// Get rowids to keep
	var keepRowIDs []int
	err = tx.tx.Model(&store.RList{}).
		Select("id").
		Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Offset(actualStart).
		Limit(actualStop-actualStart+1).
		Pluck("id", &keepRowIDs).Error
	if err != nil {
		return err
	}

	if len(keepRowIDs) == 0 {
		return nil
	}

	// Delete elements not in keep list
	result := tx.tx.Where("kid = ? AND id NOT IN ?", rkey.ID, keepRowIDs).Delete(&store.RList{})
	if result.Error != nil {
		return result.Error
	}

	// Update key metadata
	newLen := actualStop - actualStart + 1
	tx.tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        newLen,
		})

	return nil
}

// Helper methods

func (tx *Tx) delete(key string, elem any, count int, fromBack bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	// Get the key id
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Select("id").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Get rowids to delete
	order := "pos ASC"
	if fromBack {
		order = "pos DESC"
	}

	var rowIDs []int
	err = tx.tx.Model(&store.RList{}).
		Select("id").
		Where("kid = ? AND elem = ?", rkey.ID, elemb).
		Order(order).
		Limit(count).
		Pluck("id", &rowIDs).Error
	if err != nil {
		return 0, err
	}

	if len(rowIDs) == 0 {
		return 0, nil
	}

	// Delete the elements
	result := tx.tx.Where("id IN ?", rowIDs).Delete(&store.RList{})
	if result.Error != nil {
		return 0, result.Error
	}

	// Update key metadata
	if result.RowsAffected > 0 {
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", result.RowsAffected),
			})
	}

	return int(result.RowsAffected), nil
}

func (tx *Tx) insert(key string, pivot any, elem any, after bool) (int, error) {
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	// Get the key id and length
	var rkey store.RKey
	err = tx.tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if err == gorm.ErrRecordNotFound {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	// Find the pivot element
	var pivotElem store.RList
	err = tx.tx.Where("kid = ? AND elem = ?", rkey.ID, pivotb).
		Order("pos ASC").
		Limit(1).
		First(&pivotElem).Error
	if err == gorm.ErrRecordNotFound {
		return -1, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}

	// Calculate new position.
	// With int64 positions, we use adjacent positions with a gap of 2.
	// This avoids precision issues of float64 midpoints.
	// Occasional insertions between two adjacent elements use pivot+-1
	// and the caller is responsible for periodic position rebalancing if needed.
	var newPos int64
	if after {
		// Find next element after pivot
		var nextElem store.RList
		err = tx.tx.Where("kid = ? AND pos > ?", rkey.ID, pivotElem.Pos).
			Order("pos ASC").
			Limit(1).
			First(&nextElem).Error
		if err == gorm.ErrRecordNotFound {
			// No next element, use pivot pos + 1
			newPos = pivotElem.Pos + 1
		} else if err != nil {
			return 0, err
		} else {
			// Use midpoint between pivot and next (avoids collision with existing)
			newPos = pivotElem.Pos + (nextElem.Pos-pivotElem.Pos)/2
			// If they're adjacent integers, fall back to pivot+1 (gap exhaustion unlikely)
			if newPos == pivotElem.Pos {
				newPos = pivotElem.Pos + 1
			}
		}
	} else {
		// Find previous element before pivot
		var prevElem store.RList
		err = tx.tx.Where("kid = ? AND pos < ?", rkey.ID, pivotElem.Pos).
			Order("pos DESC").
			Limit(1).
			First(&prevElem).Error
		if err == gorm.ErrRecordNotFound {
			// No previous element, use pivot pos - 1
			newPos = pivotElem.Pos - 1
		} else if err != nil {
			return 0, err
		} else {
			// Use midpoint between prev and pivot (avoids collision with existing)
			newPos = prevElem.Pos + (pivotElem.Pos-prevElem.Pos)/2
			// If midpoint equals prev (adjacent positions), there's no room
			// Renumber all elements with large gaps for future insertions
			if newPos == prevElem.Pos {
				// Find all elements ordered by position
				var elems []store.RList
				tx.tx.Where("kid = ?", rkey.ID).Order("pos ASC").Find(&elems)
				
				// Renumber with large gaps: new_pos = (index + 1) * 1000
				// This gives positions 1000, 2000, 3000... leaving huge room
				// Insert BEFORE the pivot by using (pivot_index) * 1000 - 500
				for i, elem := range elems {
					newListPos := int64((i + 1) * 1000) // 1000, 2000, 3000...
					err = tx.tx.Exec(`UPDATE rlist SET pos = ? WHERE id = ?`, newListPos, elem.ID).Error
					if err != nil {
						return 0, err
					}
					// Track the pivot's new position to insert before it
					if elem.ID == pivotElem.ID {
						// Insert 500 before the pivot's new position
						newPos = int64((i + 1) * 1000 - 500)
					}
				}
			}
		}
	}

	// Insert the new element
	err = tx.tx.Create(&store.RList{
		KID:  rkey.ID,
		Pos:  newPos,
		Elem: elemb,
	}).Error
	if err != nil {
		return 0, err
	}

	// Update key metadata
	var newLen int64
	tx.tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        gorm.Expr("klen + 1"),
		})

	// Get the new length
	tx.tx.Model(&store.RKey{}).
		Select("klen").
		Where("id = ?", rkey.ID).
		Scan(&newLen)

	return int(newLen), nil
}

func (tx *Tx) push(key string, elem any, back bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	var newLen int64
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Check if key exists and validate type
		var rkey store.RKey
		err := txInner.Model(&store.RKey{}).
			Select("id", "ktype", "klen").
			Where("kdb = ? AND kname = ?", tx.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error

		switch err {
		case nil:
			// Key exists, check type
			if rkey.KType != 2 {
				return core.ErrKeyType
			}
			// Update version and mtime, len will be incremented after element insert
			if err := txInner.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		case gorm.ErrRecordNotFound:
			// Key does not exist, create new key
			rkey = store.RKey{
				KDB:        tx.dbIdx,
				KName:      key,
				KType:      2,
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

		// Calculate new position
		var newPos int64
		if back {
			// Append: find max pos and add 1
			var maxPos *int64
			txInner.Model(&store.RList{}).
				Select("MAX(pos)").
				Where("kid = ?", rkey.ID).
				Scan(&maxPos)
			if maxPos != nil {
				newPos = *maxPos + 1
			}
		} else {
			// Prepend: find min pos and subtract 1
			var minPos *int64
			txInner.Model(&store.RList{}).
				Select("MIN(pos)").
				Where("kid = ?", rkey.ID).
				Scan(&minPos)
			if minPos != nil {
				newPos = *minPos - 1
			}
		}

		// Insert the element
		if err := txInner.Create(&store.RList{
			KID:  rkey.ID,
			Pos:  newPos,
			Elem: elemb,
		}).Error; err != nil {
			return err
		}

		// Increment len after successful element insert
		if err := txInner.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Update("klen", gorm.Expr("klen + 1")).Error; err != nil {
			return err
		}

		// Get the new length
		txInner.Model(&store.RKey{}).
			Select("klen").
			Where("id = ?", rkey.ID).
			Scan(&newLen)

		return nil
	})

	return int(newLen), err
}
