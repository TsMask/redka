// Package rlist is a database-backed list repository.
// It provides methods to interact with lists in the database.
package rlist

import (
	"context"
	"errors"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
)

// listPositionInterval is the interval between list positions.
// Using a larger interval (1000) reduces the frequency of position
// reallocation during LINSERT operations, significantly improving
// performance in high-concurrency scenarios.
const listPositionInterval = 1000

// DB is a database-backed list repository.
// A list is an ordered collection of strings.
// Use the list repository to work with individual lists
// and their elements.
//
// This is a simplified architecture that directly uses store.Store
// without additional transaction wrappers. Each method handles
// its own transactions internally when needed.
type DB struct {
	store *store.Store
	dbIdx int
}

// New connects to the list repository.
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

// Delete deletes all occurrences of an element from a list.
// Returns the number of elements deleted.
// Does nothing if the key does not exist or is not a list.
// This method uses transaction with row-level locking to prevent race conditions.
func (d *DB) Delete(key string, elem any) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var n int
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()

		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
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

		result := tx.Where("kid = ? AND elem = ?", rkey.ID, elemb).Delete(&store.RList{})
		if result.Error != nil {
			return result.Error
		}

		n = int(result.RowsAffected)
		if result.RowsAffected > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen - ?", result.RowsAffected),
				}).Error
		}
		return nil
	})

	return n, err
}

// DeleteBack deletes the first count occurrences of an element
// from a list, starting from the back. Count must be positive.
// Returns the number of elements deleted.
func (d *DB) DeleteBack(key string, elem any, count int) (int, error) {
	return d.delete(key, elem, count, true)
}

// DeleteFront deletes the first count occurrences of an element
// from a list, starting from the front. Count must be positive.
// Returns the number of elements deleted.
func (d *DB) DeleteFront(key string, elem any, count int) (int, error) {
	return d.delete(key, elem, count, false)
}

// Get returns an element from a list by index (0-based).
// Negative index count from the end of the list
// (-1 is the last element, -2 is the second last, etc.)
func (d *DB) Get(key string, idx int) (core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	actualIdx := idx
	if idx < 0 {
		actualIdx = rkey.KLen + idx
	}
	if actualIdx < 0 || actualIdx >= rkey.KLen {
		return nil, core.ErrNotFound
	}

	var rlist store.RList
	err = d.store.DB.Where("kid = ?", rkey.ID).
		Order("pos ASC").
		Offset(actualIdx).
		Limit(1).
		First(&rlist).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, core.ErrNotFound
	}
	if err != nil {
		return nil, err
	}

	return core.Value(rlist.Elem), nil
}

// InsertAfter inserts an element after another element.
// Returns the new length of the list.
func (d *DB) InsertAfter(key string, pivot any, elem any) (int, error) {
	return d.insert(key, pivot, elem, true)
}

// InsertBefore inserts an element before another element.
// Returns the new length of the list.
func (d *DB) InsertBefore(key string, pivot any, elem any) (int, error) {
	return d.insert(key, pivot, elem, false)
}

// Len returns the number of elements in a list.
func (d *DB) Len(key string) (int, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Select("klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return rkey.KLen, nil
}

// PopBack removes and returns the last element from a list.
func (d *DB) PopBack(key string) (core.Value, error) {
	var elem []byte
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		if rkey.KLen == 0 {
			return core.ErrNotFound
		}

		var rlist store.RList
		err = tx.Where("kid = ?", rkey.ID).
			Order("pos DESC").
			Limit(1).
			First(&rlist).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		elem = rlist.Elem

		if err := tx.Where("id = ?", rlist.ID).Delete(&store.RList{}).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			}).Error
	})

	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

// PopFront removes and returns the first element from a list.
func (d *DB) PopFront(key string) (core.Value, error) {
	var elem []byte
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		if rkey.KLen == 0 {
			return core.ErrNotFound
		}

		var rlist store.RList
		err = tx.Where("kid = ?", rkey.ID).
			Order("pos ASC").
			Limit(1).
			First(&rlist).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		elem = rlist.Elem

		if err := tx.Where("id = ?", rlist.ID).Delete(&store.RList{}).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			}).Error
	})

	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

// PopBackPushFront removes the last element from src list
// and prepends it to dest list. Returns the popped element.
func (d *DB) PopBackPushFront(src, dest string) (core.Value, error) {
	var elem []byte
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var srcKey store.RKey
		if err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, src).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&srcKey).Error; err != nil {
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			return err
		}

		if srcKey.KLen == 0 {
			return core.ErrNotFound
		}

		var srcElem store.RList
		if err := tx.Where("kid = ?", srcKey.ID).
			Order("pos DESC").
			Limit(1).
			First(&srcElem).Error; err != nil {
			return err
		}
		elem = srcElem.Elem

		if err := tx.Where("id = ?", srcElem.ID).Delete(&store.RList{}).Error; err != nil {
			return err
		}

		tx.Model(&store.RKey{}).
			Where("id = ?", srcKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - 1"),
			})

		var destKey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "ktype", "klen").
			Where("kdb = ? AND kname = ?", d.dbIdx, dest).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&destKey).Error

		switch {
		case err == nil:
			if destKey.KType != 2 {
				return core.ErrKeyType
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			destKey = store.RKey{
				KDB:        d.dbIdx,
				KName:      dest,
				KType:      2,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := tx.Create(&destKey).Error; err != nil {
				return err
			}
		default:
			return err
		}

		var minPos *int64
		if err := tx.Model(&store.RList{}).
			Select("MIN(pos)").
			Where("kid = ?", destKey.ID).
			Scan(&minPos).Error; err != nil {
			return err
		}

		newPos := int64(0)
		if minPos != nil {
			newPos = *minPos - 1
		}

		if err := tx.Create(&store.RList{
			KID:  destKey.ID,
			Pos:  newPos,
			Elem: elem,
		}).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen + 1"),
			}).Error
	})

	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

// PushBack appends an element to the back of a list.
// Returns the new length of the list.
func (d *DB) PushBack(key string, elem any) (int, error) {
	return d.push(key, elem, true)
}

// PushFront prepends an element to the front of a list.
// Returns the new length of the list.
func (d *DB) PushFront(key string, elem any) (int, error) {
	return d.push(key, elem, false)
}

// Range returns a slice of a list.
func (d *DB) Range(key string, start, stop int) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return []core.Value{}, nil
	}
	if err != nil {
		return nil, err
	}

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

	var rlists []store.RList
	err = d.store.DB.Where("kid = ?", rkey.ID).
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
func (d *DB) Set(key string, idx int, elem any) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}

	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		actualIdx := idx
		if idx < 0 {
			actualIdx = rkey.KLen + idx
		}
		if actualIdx < 0 || actualIdx >= rkey.KLen {
			return core.ErrNotFound
		}

		var targetElem store.RList
		err = tx.Where("kid = ?", rkey.ID).
			Order("pos ASC").
			Offset(actualIdx).
			Limit(1).
			First(&targetElem).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		result := tx.Model(&store.RList{}).
			Where("id = ?", targetElem.ID).
			Update("elem", elemb)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return core.ErrNotFound
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			}).Error
	})
}

// Trim trims a list so that it contains only the specified range of elements.
func (d *DB) Trim(key string, start, stop int) error {
	return d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

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
			result := tx.Where("kid = ?", rkey.ID).Delete(&store.RList{})
			if result.Error != nil {
				return result.Error
			}
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        0,
				}).Error
		}

		var keepRowIDs []int
		err = tx.Model(&store.RList{}).
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

		result := tx.Where("kid = ? AND id NOT IN ?", rkey.ID, keepRowIDs).Delete(&store.RList{})
		if result.Error != nil {
			return result.Error
		}

		newLen := actualStop - actualStart + 1
		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        newLen,
			}).Error
	})
}

// Helper methods

func (d *DB) delete(key string, elem any, count int, fromBack bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var deleted int64
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			deleted = 0
			return nil
		}
		if err != nil {
			return err
		}

		order := "pos ASC"
		if fromBack {
			order = "pos DESC"
		}

		var rowIDs []int
		err = tx.Model(&store.RList{}).
			Select("id").
			Where("kid = ? AND elem = ?", rkey.ID, elemb).
			Order(order).
			Limit(count).
			Pluck("id", &rowIDs).Error
		if err != nil {
			return err
		}

		if len(rowIDs) == 0 {
			deleted = 0
			return nil
		}

		result := tx.Where("id IN ?", rowIDs).Delete(&store.RList{})
		if result.Error != nil {
			return result.Error
		}

		deleted = result.RowsAffected
		if result.RowsAffected > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen - ?", result.RowsAffected),
				}).Error
		}
		return nil
	})

	return int(deleted), err
}

func (d *DB) insert(key string, pivot any, elem any, after bool) (int, error) {
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var newLen int64
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "klen").
			Where("kdb = ? AND kname = ? AND ktype = 2", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		var pivotElem store.RList
		err = tx.Where("kid = ? AND elem = ?", rkey.ID, pivotb).
			Order("pos ASC").
			Limit(1).
			First(&pivotElem).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}

		var newPos int64
		if after {
			var nextElem store.RList
			err = tx.Where("kid = ? AND pos > ?", rkey.ID, pivotElem.Pos).
				Order("pos ASC").
				Limit(1).
				First(&nextElem).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				newPos = pivotElem.Pos + 1
			} else if err != nil {
				return err
			} else {
				newPos = pivotElem.Pos + (nextElem.Pos-pivotElem.Pos)/2
				if newPos == pivotElem.Pos {
					newPos = pivotElem.Pos + 1
				}
			}
		} else {
			var prevElem store.RList
			err = tx.Where("kid = ? AND pos < ?", rkey.ID, pivotElem.Pos).
				Order("pos DESC").
				Limit(1).
				First(&prevElem).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				newPos = pivotElem.Pos - 1
			} else if err != nil {
				return err
			} else {
				newPos = prevElem.Pos + (pivotElem.Pos-prevElem.Pos)/2
				if newPos == prevElem.Pos {
					var elems []store.RList
					tx.Where("kid = ?", rkey.ID).Order("pos ASC").Find(&elems)

					for i, elem := range elems {
						newListPos := int64((i + 1) * 1000)
						err = tx.Exec(`UPDATE rlist SET pos = ? WHERE id = ?`, newListPos, elem.ID).Error
						if err != nil {
							return err
						}
						if elem.ID == pivotElem.ID {
							newPos = int64((i+1)*1000 - 500)
						}
					}
				}
			}
		}

		err = tx.Create(&store.RList{
			KID:  rkey.ID,
			Pos:  newPos,
			Elem: elemb,
		}).Error
		if err != nil {
			return err
		}

		tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen + 1"),
			})

		if err := tx.Model(&store.RKey{}).
			Select("klen").
			Where("id = ?", rkey.ID).
			Scan(&newLen).Error; err != nil {
			return err
		}

		return nil
	})

	return int(newLen), err
}

func (d *DB) push(key string, elem any, back bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var newLen int64
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Select("id", "ktype", "klen").
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			Clauses(store.ForUpdate()).
			First(&rkey).Error

		switch {
		case err == nil:
			if rkey.KType != 2 {
				return core.ErrKeyType
			}
			if err := tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error; err != nil {
				return err
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      2,
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

		var newPos int64
		if back {
			var maxPos *int64
			if err := tx.Model(&store.RList{}).
				Select("MAX(pos)").
				Where("kid = ?", rkey.ID).
				Scan(&maxPos).Error; err != nil {
				return err
			}
			if maxPos != nil {
				newPos = *maxPos + listPositionInterval // Use larger interval to reduce reallocation
			} else {
				newPos = listPositionInterval // First element
			}
		} else {
			var minPos *int64
			if err := tx.Model(&store.RList{}).
				Select("MIN(pos)").
				Where("kid = ?", rkey.ID).
				Scan(&minPos).Error; err != nil {
				return err
			}
			if minPos != nil {
				newPos = *minPos - listPositionInterval // Use larger interval to reduce reallocation
			} else {
				newPos = listPositionInterval // First element
			}
		}

		if err := tx.Create(&store.RList{
			KID:  rkey.ID,
			Pos:  newPos,
			Elem: elemb,
		}).Error; err != nil {
			return err
		}

		tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Updates(map[string]any{
				"klen": gorm.Expr("klen + 1"),
			})

		if err := tx.Model(&store.RKey{}).
			Select("klen").
			Where("id = ?", rkey.ID).
			Scan(&newLen).Error; err != nil {
			return err
		}

		return nil
	})

	return int(newLen), err
}
