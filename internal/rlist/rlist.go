// Package rlist is a database-backed list repository.
// It provides methods to interact with lists in the database.
package rlist

import (
	"bytes"
	"context"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	ctx   context.Context
}

// New connects to the list repository.
// Does not create the database schema.
func New(ctx context.Context, s *store.Store) *DB {
	return &DB{store: s, dbIdx: 0, ctx: ctx}
}

// WithContext returns a copy of DB bound to ctx.
func (d *DB) WithContext(ctx context.Context) *DB {
	newDB := *d
	newDB.ctx = ctx
	return &newDB
}

// db returns the store view bound to this DB's context.
func (d *DB) db() *gorm.DB {
	return d.store.WithContext(d.ctx).DB
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
	err = d.store.TransactionWrite(d.ctx, d.writeScope("delete", key),
		func(tx *gorm.DB, _ store.Dialect) error {
			now := time.Now().UnixMilli()

			var rkey store.RKey
			err := tx.Model(&store.RKey{}).
				Select("id").
				Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
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
	err := d.db().Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
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
	err = d.db().Where("kid = ?", rkey.ID).
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
	err := d.db().Model(&store.RKey{}).
		Select("klen").
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
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
	elem := []byte(nil)
	item := listPopItem{elem: &elem}
	err := d.pop(key, false, item)
	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

// PopFront removes and returns the first element from a list.
func (d *DB) PopFront(key string) (core.Value, error) {
	elem := []byte(nil)
	item := listPopItem{elem: &elem}
	err := d.pop(key, true, item)
	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

type listPopItem struct {
	elem *[]byte
}

func (d *DB) pop(key string, front bool, item listPopItem) error {
	return d.store.TransactionWriteItems(d.ctx,
		d.writeScope("pop", key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				order := "pos DESC"
				if front {
					order = "pos ASC"
				}
				var rows []struct {
					KeyID int    `gorm:"column:key_id"`
					RowID int    `gorm:"column:row_id"`
					Elem  []byte `gorm:"column:elem"`
				}
				if err := tx.Model(&store.RKey{}).
					Select("rkey.id AS key_id, rlist.id AS row_id, rlist.elem").
					Joins("JOIN rlist ON rlist.kid = rkey.id").
					Where("rkey.kdb = ? AND rkey.kname = ? AND rkey.ktype = ?",
						d.dbIdx, key, core.TypeList.Value()).
					Order(order).
					Limit(len(items)).
					Find(&rows).Error; err != nil {
					return err
				}
				if len(rows) == 0 {
					var rkey store.RKey
					err := tx.Model(&store.RKey{}).
						Select("id", "klen").
						Where("kdb = ? AND kname = ? AND ktype = ?",
							d.dbIdx, key, core.TypeList.Value()).
						First(&rkey).Error
					if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && rkey.KLen == 0) {
						if len(items) == 1 {
							return core.ErrNotFound
						}
						return nil
					}
					if err != nil {
						return err
					}
					if len(items) == 1 {
						return core.ErrNotFound
					}
					return nil
				}

				keyID := rows[0].KeyID
				for i, row := range rows {
					if i < len(items) {
						*items[i].(listPopItem).elem = row.Elem
					}
				}
				ids := make([]int, len(rows))
				for i, row := range rows {
					ids[i] = row.RowID
				}
				if err := tx.Where("id IN ?", ids).Delete(&store.RList{}).Error; err != nil {
					return err
				}

				return tx.Model(&store.RKey{}).
					Where("id = ?", keyID).
					Updates(map[string]any{
						"kver":        gorm.Expr("kver + ?", len(rows)),
						"modified_at": now,
						"klen":        gorm.Expr("klen - ?", len(rows)),
					}).Error
			}
			return run(tx)
		})
}

// PopBackPushFront removes the last element from src list
// and prepends it to dest list. Returns the popped element.
func (d *DB) PopBackPushFront(src, dest string) (core.Value, error) {
	if src == dest {
		return d.popBackPushFrontSingle(src, dest)
	}

	elem := []byte(nil)
	item := listMoveItem{elem: &elem}
	err := d.store.TransactionWriteItems(d.ctx, d.moveScope(src, dest), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				firstName, secondName := src, dest
				firstIsSrc := true
				if dest < src {
					firstName, secondName = dest, src
					firstIsSrc = false
				}
				lockOrCreate := func(name string) (store.RKey, error) {
					key, err := store.LockKey(tx, dialect, d.dbIdx, name)
					if !errors.Is(err, gorm.ErrRecordNotFound) {
						return key, err
					}
					return store.EnsureKeyLocked(tx, dialect, d.dbIdx,
						name, core.TypeList.Value(), now, 0)
				}

				var srcKey, destKey store.RKey
				if firstIsSrc {
					var err error
					srcKey, err = store.LockKey(tx, dialect, d.dbIdx, firstName)
					if errors.Is(err, gorm.ErrRecordNotFound) {
						if len(items) == 1 {
							return core.ErrNotFound
						}
						return nil
					}
					if err != nil {
						return err
					}
					destKey, err = lockOrCreate(secondName)
					if err != nil {
						return err
					}
				} else {
					var err error
					destKey, err = lockOrCreate(firstName)
					if err != nil {
						return err
					}
					srcKey, err = store.LockKey(tx, dialect, d.dbIdx, secondName)
					if errors.Is(err, gorm.ErrRecordNotFound) {
						if len(items) == 1 {
							return core.ErrNotFound
						}
						return nil
					}
					if err != nil {
						return err
					}
				}
				if srcKey.KType != core.TypeList.Value() ||
					destKey.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
				if srcKey.KLen == 0 {
					if len(items) == 1 {
						return core.ErrNotFound
					}
					return nil
				}

				var rows []store.RList
				if err := tx.Where("kid = ?", srcKey.ID).
					Order("pos DESC").
					Limit(len(items)).
					Find(&rows).Error; err != nil {
					return err
				}
				if len(rows) == 0 {
					if len(items) == 1 {
						return core.ErrNotFound
					}
					return nil
				}

				headPos := int64(0)
				tailPos := int64(0)
				if destKey.KListHead != nil {
					headPos = *destKey.KListHead
				}
				if destKey.KListTail != nil {
					tailPos = *destKey.KListTail
				}
				if destKey.KLen > 0 && destKey.KListHead == nil {
					if err := tx.Model(&store.RList{}).
						Select("MIN(pos)").
						Where("kid = ?", destKey.ID).
						Scan(&headPos).Error; err != nil {
						return err
					}
				}
				if destKey.KLen > 0 && destKey.KListTail == nil {
					if err := tx.Model(&store.RList{}).
						Select("MAX(pos)").
						Where("kid = ?", destKey.ID).
						Scan(&tailPos).Error; err != nil {
						return err
					}
				}

				ids := make([]int, len(rows))
				positionCases := "CASE id"
				positionCase := " WHEN ? THEN ?"
				if dialect == store.DialectPostgres {
					positionCase = " WHEN ? THEN CAST(? AS BIGINT)"
				}
				for i, row := range rows {
					ids[i] = row.ID
					if i < len(items) {
						*items[i].(listMoveItem).elem = row.Elem
					}
					positionCases += positionCase
				}
				positionCases += " END"
				positionArgs := make([]any, 0, len(rows)*2)
				for i, row := range rows {
					positionArgs = append(positionArgs, row.ID,
						headPos-int64(i+1)*listPositionInterval)
				}
				result := tx.Model(&store.RList{}).
					Where("id IN ?", ids).
					Updates(map[string]any{
						"kid": destKey.ID,
						"pos": gorm.Expr(positionCases, positionArgs...),
					})
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected != int64(len(rows)) {
					return errors.New("list move affected an unexpected number of rows")
				}

				if err := tx.Model(&store.RKey{}).
					Where("id = ?", srcKey.ID).
					Updates(map[string]any{
						"kver":        gorm.Expr("kver + ?", len(rows)),
						"modified_at": now,
						"klen":        gorm.Expr("klen - ?", len(rows)),
					}).Error; err != nil {
					return err
				}
				newHead := headPos - int64(len(rows))*listPositionInterval
				newTail := tailPos
				if destKey.KLen == 0 {
					newTail = headPos - listPositionInterval
				}
				return tx.Model(&store.RKey{}).
					Where("id = ?", destKey.ID).
					Updates(map[string]any{
						"kver":        gorm.Expr("kver + ?", len(rows)),
						"modified_at": now,
						"klen":        gorm.Expr("klen + ?", len(rows)),
						"klist_head":  newHead,
						"klist_tail":  newTail,
					}).Error
			}
			if len(items) == 1 && !d.store.InTx() {
				return tx.Transaction(func(tx *gorm.DB) error {
					return run(tx)
				})
			}
			return run(tx)
		})
	if err != nil {
		return nil, err
	}
	return core.Value(elem), nil
}

type listMoveItem struct {
	elem *[]byte
}

func (d *DB) popBackPushFrontSingle(src, dest string) (core.Value, error) {
	var elem []byte
	err := d.store.TransactionWrite(d.ctx, d.moveScope(src, dest),
		func(tx *gorm.DB, dialect store.Dialect) error {
			now := time.Now().UnixMilli()

			// 死锁预防：按键名排序后加锁，确保固定的锁定顺序
			// 这样可以避免两个并发事务以相反顺序锁定相同的键而导致死锁
			keys := []string{src, dest}
			sort.Strings(keys)
			firstKey := keys[0]
			secondKey := keys[1]

			// A missing source is a no-op and must not create either key.
			var srcExists store.RKey
			err := tx.Model(&store.RKey{}).
				Select("id").
				Where("kdb = ? AND kname = ?", d.dbIdx, src).
				First(&srcExists).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}

			// Avoid a locking read for a missing key: on MySQL it takes a gap
			// lock before the key-creation mutex can be acquired.
			var firstKeyRecord store.RKey
			err = nil
			if firstKey == src {
				firstKeyRecord, err = store.LockKey(tx, dialect, d.dbIdx, firstKey)
			} else {
				firstKeyRecord, err = store.EnsureKeyLocked(tx, dialect, d.dbIdx,
					firstKey, core.TypeList.Value(), now, 0)
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}

			// 根据 firstKey 是 src 还是 dest 来处理
			var srcKey store.RKey
			var destKey store.RKey

			if firstKey == src {
				// firstKey 是 src
				if firstKeyRecord.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
				if firstKeyRecord.KLen == 0 {
					return core.ErrNotFound
				}
				srcKey = firstKeyRecord

				// 检查或创建 dest
				var err error
				destKey, err = store.EnsureKeyLocked(tx, dialect, d.dbIdx,
					dest, core.TypeList.Value(), now, 0)
				if err != nil {
					return err
				}
				if destKey.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
			} else {
				// firstKey 是 dest
				if firstKeyRecord.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
				destKey = firstKeyRecord

				// 检查 src
				var err error
				srcKey, err = store.LockKey(tx, dialect, d.dbIdx, secondKey)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return core.ErrNotFound
				}
				if err != nil {
					return err
				}
				if srcKey.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
				if srcKey.KLen == 0 {
					return core.ErrNotFound
				}
			}

			// 从源列表弹出最后一个元素
			srcElemQuery := tx.Model(&store.RList{}).
				Where("kid = ?", srcKey.ID)
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				srcElemQuery = srcElemQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			var srcElem store.RList
			if err := srcElemQuery.
				Order("pos DESC").
				Limit(1).
				First(&srcElem).Error; err != nil {
				return err
			}
			elem = srcElem.Elem

			if err := tx.Where("id = ?", srcElem.ID).Delete(&store.RList{}).Error; err != nil {
				return err
			}

			if err := tx.Model(&store.RKey{}).
				Where("id = ?", srcKey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen - 1"),
				}).Error; err != nil {
				return err
			}

			// Lock the destination's current head element. A plain MIN(pos) read
			// can use an MVCC snapshot after waiting for the destination key lock,
			// causing concurrent transactions to choose the same position.
			destHeadQuery := tx.Model(&store.RList{}).
				Where("kid = ?", destKey.ID)
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				destHeadQuery = destHeadQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			var destHead store.RList
			err = destHeadQuery.Order("pos ASC").Limit(1).First(&destHead).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			newPos := int64(0)
			if err == nil {
				newPos = destHead.Pos - 1
			}

			// 插入到目标列表前端
			if err := tx.Create(&store.RList{
				KID:  destKey.ID,
				Pos:  newPos,
				Elem: elem,
			}).Error; err != nil {
				return err
			}

			// 更新目标列表长度
			return tx.Model(&store.RKey{}).
				Where("id = ?", destKey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
					"klen":        gorm.Expr("klen + 1"),
					"klist_head": gorm.Expr(
						"(SELECT MIN(pos) FROM rlist WHERE rlist.kid = ?)", destKey.ID),
					"klist_tail": gorm.Expr(
						"(SELECT MAX(pos) FROM rlist WHERE rlist.kid = ?)", destKey.ID),
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
	return d.pushMany(key, []any{elem}, true)
}

// PushFront prepends an element to the front of a list.
// Returns the new length of the list.
func (d *DB) PushFront(key string, elem any) (int, error) {
	return d.pushMany(key, []any{elem}, false)
}

// PushBackMany appends elements to the back of a list in one write.
func (d *DB) PushBackMany(key string, elems ...any) (int, error) {
	return d.pushMany(key, elems, true)
}

// PushFrontMany prepends elements to the front of a list in one write.
// Elements retain command order, so LPUSH a b c produces [c, b, a].
func (d *DB) PushFrontMany(key string, elems ...any) (int, error) {
	return d.pushMany(key, elems, false)
}

func (d *DB) writeScope(op, key string) string {
	return "list:" + op + ":" + strconv.Itoa(d.dbIdx) + "\x00" + key
}

func (d *DB) moveScope(src, dest string) string {
	return d.writeScope("move", src+"\x00"+dest)
}

// Range returns a slice of a list.
func (d *DB) Range(key string, start, stop int) ([]core.Value, error) {
	now := time.Now().UnixMilli()

	var rkey store.RKey
	err := d.db().Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
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
	err = d.db().Where("kid = ?", rkey.ID).
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

	item := listSetWriteItem{index: idx, elem: elemb}
	return d.store.TransactionWriteItems(d.ctx,
		d.writeScope("set", key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			needsKey := dialect != store.DialectMySQL
			for _, raw := range items {
				if raw.(listSetWriteItem).index < 0 {
					needsKey = true
					break
				}
			}

			var keyID int
			keyLen := -1
			if needsKey {
				var rkey store.RKey
				keyQuery := tx.Model(&store.RKey{}).
					Select("id", "klen").
					Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value())
				if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
					keyQuery = keyQuery.Clauses(clause.Locking{Strength: "UPDATE"})
				}
				err := keyQuery.First(&rkey).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return core.ErrNotFound
				}
				if err != nil {
					return err
				}
				keyID = rkey.ID
				keyLen = rkey.KLen
			}

			// Writes to the same index are independent of earlier writes; only
			// the value from the last request must remain.
			final := make(map[int][]byte)
			for _, raw := range items {
				set := raw.(listSetWriteItem)
				actual := set.index
				if actual < 0 {
					actual = keyLen + actual
				}
				if actual < 0 || (keyLen >= 0 && actual >= keyLen) {
					return core.ErrNotFound
				}
				final[actual] = set.elem
			}

			for actual, elem := range final {
				if err := d.updateListPosition(tx, dialect, keyID, key, actual, elem); err != nil {
					return err
				}
			}
			return nil
		})
}

type listSetWriteItem struct {
	index int
	elem  []byte
}

func (d *DB) updateListPosition(tx *gorm.DB, dialect store.Dialect,
	keyID int, key string, index int, elem []byte) error {
	if dialect == store.DialectSQLite {
		var target store.RList
		err := tx.Select("id").
			Where("kid = ?", keyID).
			Order("pos ASC").
			Offset(index).
			Limit(1).
			First(&target).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		if err != nil {
			return err
		}
		return tx.Model(&store.RList{}).
			Where("id = ?", target.ID).
			Update("elem", elem).Error
	}

	sqlText := `
		UPDATE rlist AS l
		SET elem = ?
		WHERE l.id = (
			SELECT id FROM rlist WHERE kid = ? ORDER BY pos ASC LIMIT 1 OFFSET ?
		)`
	args := []any{elem, keyID, index}
	if dialect == store.DialectMySQL {
		if keyID == 0 {
			sqlText = `
				UPDATE rlist AS l
				JOIN (
					SELECT rl.id
					FROM rlist AS rl
					JOIN rkey AS k ON k.id = rl.kid
					WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ?
					ORDER BY rl.pos ASC LIMIT 1 OFFSET ?
				) AS target ON target.id = l.id
				SET l.elem = ?`
			args = []any{d.dbIdx, key, core.TypeList.Value(), index, elem}
		} else {
			sqlText = `
			UPDATE rlist AS l
			JOIN (
				SELECT id FROM rlist WHERE kid = ? ORDER BY pos ASC LIMIT 1 OFFSET ?
			) AS target ON target.id = l.id
			SET l.elem = ?`
			args = []any{keyID, index, elem}
		}
	}
	result := tx.Exec(sqlText, args...)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return core.ErrNotFound
	}
	return nil
}

// Trim trims a list so that it contains only the specified range of elements.
func (d *DB) Trim(key string, start, stop int) error {
	item := listTrimItem{start: start, stop: stop}
	return d.store.TransactionWriteItems(d.ctx,
		d.writeScope("trim", key), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			groups := make(map[string][]listTrimItem)
			order := make([]string, 0)
			for _, raw := range items {
				trim := raw.(listTrimItem)
				signature := strconv.Itoa(trim.start) + ":" + strconv.Itoa(trim.stop)
				if _, exists := groups[signature]; !exists {
					order = append(order, signature)
				}
				groups[signature] = append(groups[signature], trim)
			}

			// Identical LTRIM arguments are idempotent; preserve the order of
			// distinct argument groups but execute each group only once.
			for _, signature := range order {
				group := groups[signature]
				if err := d.trimOnce(tx, key, group[len(group)-1]); err != nil {
					return err
				}
			}
			return nil
		})
}

type listTrimItem struct {
	start int
	stop  int
}

func (d *DB) trimOnce(tx *gorm.DB, key string, trim listTrimItem) error {
	now := time.Now().UnixMilli()
	var rkey store.RKey
	err := tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	actualStart := trim.start
	actualStop := trim.stop
	if trim.start < 0 {
		actualStart = rkey.KLen + trim.start
	}
	if trim.stop < 0 {
		actualStop = rkey.KLen + trim.stop
	}
	if actualStart < 0 {
		actualStart = 0
	}
	if actualStop >= rkey.KLen {
		actualStop = rkey.KLen - 1
	}

	if actualStart == 0 && actualStop == rkey.KLen-1 {
		return nil
	}
	if actualStart > actualStop {
		result := tx.Where("kid = ?", rkey.ID).Delete(&store.RList{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return nil
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
	if result.RowsAffected == 0 {
		return nil
	}
	newLen := actualStop - actualStart + 1
	return tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        newLen,
		}).Error
}

// Helper methods

func (d *DB) delete(key string, elem any, count int, fromBack bool) (int, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	deleted := 0
	item := listDeleteCountItem{
		elem: elemb, count: count, fromBack: fromBack, deleted: &deleted,
	}
	err = d.store.TransactionWriteItems(d.ctx,
		d.writeScope("delete-count", key), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			var rkey store.RKey
			err := tx.Model(&store.RKey{}).
				Select("id").
				Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value()).
				First(&rkey).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				for _, raw := range items {
					*raw.(listDeleteCountItem).deleted = 0
				}
				return nil
			}
			if err != nil {
				return err
			}

			groups := make(map[string][]listDeleteCountItem)
			order := make([]string, 0)
			for _, raw := range items {
				deleteItem := raw.(listDeleteCountItem)
				signature := listDeleteSignature(deleteItem)
				if _, exists := groups[signature]; !exists {
					order = append(order, signature)
				}
				groups[signature] = append(groups[signature], deleteItem)
			}

			totalDeleted := 0
			updatedCommands := 0
			for _, signature := range order {
				group := groups[signature]
				deleted, err := d.deleteCountMany(tx, rkey.ID, group)
				if err != nil {
					return err
				}
				for _, n := range deleted {
					totalDeleted += n
					if n > 0 {
						updatedCommands++
					}
				}
			}
			if totalDeleted == 0 {
				return nil
			}
			now := time.Now().UnixMilli()
			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"kver":        gorm.Expr("kver + ?", updatedCommands),
					"modified_at": now,
					"klen":        gorm.Expr("klen - ?", totalDeleted),
				}).Error
		})

	return deleted, err
}

func (d *DB) deleteCountMany(tx *gorm.DB, keyID int,
	group []listDeleteCountItem) ([]int, error) {
	first := group[0]
	results := make([]int, len(group))
	if first.count == 0 {
		return results, nil
	}

	requested := first.count
	for range group[1:] {
		if requested > 0 && requested > math.MaxInt-first.count {
			requested = math.MaxInt
			break
		}
		if requested < 0 && requested < math.MinInt-first.count {
			requested = math.MinInt
			break
		}
		requested += first.count
	}
	if requested == 0 {
		return results, nil
	}
	limit := requested
	if limit < 0 {
		if limit == math.MinInt {
			limit = math.MaxInt
		} else {
			limit = -limit
		}
	}

	order := "pos ASC"
	if first.count < 0 {
		order = "pos DESC"
	}
	var rowIDs []int
	err := tx.Model(&store.RList{}).
		Select("id").
		Where("kid = ? AND elem = ?", keyID, first.elem).
		Order(order).
		Limit(limit).
		Pluck("id", &rowIDs).Error
	if err != nil {
		return nil, err
	}
	available := len(rowIDs)
	count := first.count
	if count < 0 {
		count = -count
	}
	for i := range results {
		results[i] = available - i*count
		if results[i] > count {
			results[i] = count
		}
		if results[i] < 0 {
			results[i] = 0
		}
		*group[i].deleted = results[i]
	}

	if available == 0 {
		return results, nil
	}
	if err := tx.Where("id IN ?", rowIDs).Delete(&store.RList{}).Error; err != nil {
		return nil, err
	}
	return results, nil
}

func listDeleteSignature(item listDeleteCountItem) string {
	fromBack := "0"
	if item.fromBack {
		fromBack = "1"
	}
	return strconv.Itoa(len(item.elem)) + ":" + string(item.elem) + ":" +
		strconv.Itoa(item.count) + ":" + fromBack
}

type listDeleteCountItem struct {
	elem     []byte
	count    int
	fromBack bool
	deleted  *int
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

	newLen := 0
	item := listInsertItem{
		pivot: pivotb, elem: elemb, after: after, length: &newLen,
	}
	err = d.store.TransactionWriteItems(d.ctx,
		d.writeScope("insert", key), item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				inserts := make([]listInsertItem, len(items))
				same := true
				for i, raw := range items {
					inserts[i] = raw.(listInsertItem)
					if i > 0 && (!bytes.Equal(inserts[i].pivot, inserts[0].pivot) ||
						!bytes.Equal(inserts[i].elem, inserts[0].elem) ||
						inserts[i].after != inserts[0].after) {
						same = false
					}
				}
				if same {
					return d.insertSame(tx, dialect, key, inserts)
				}

				// Mixed insert arguments are rare; retain the general path in
				// one transaction without re-entering the write committer.
				txDB := &DB{store: store.FromTx(dialect, tx), dbIdx: d.dbIdx}
				for _, insert := range inserts {
					n, err := txDB.insertSlow(key, insert.pivot, insert.elem, insert.after)
					*insert.length = n
					if err != nil {
						return err
					}
				}
				return nil
			}
			if len(items) == 1 && !d.store.InTx() {
				return tx.Transaction(func(tx *gorm.DB) error {
					return run(tx)
				})
			}
			return run(tx)
		})
	if errors.Is(err, core.ErrNotFound) {
		return 0, err
	}
	return newLen, err
}

type listInsertItem struct {
	pivot  []byte
	elem   []byte
	after  bool
	length *int
}

func (d *DB) insertSame(tx *gorm.DB, dialect store.Dialect,
	key string, items []listInsertItem) error {
	first := items[0]
	keyQuery := tx.Model(&store.RKey{}).
		Select("id", "klen").
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value())
	if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
		keyQuery = keyQuery.Clauses(clause.Locking{Strength: "UPDATE"})
	}
	var rkey store.RKey
	if err := keyQuery.First(&rkey).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		return err
	}

	var pivotRow store.RList
	err := tx.Where("kid = ? AND elem = ?", rkey.ID, first.pivot).
		Order("pos ASC").Limit(1).First(&pivotRow).Error
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return core.ErrNotFound
		}
		return err
	}

	neighborPos := (*int64)(nil)
	var neighborQuery *gorm.DB
	if first.after {
		neighborQuery = tx.Where("kid = ? AND pos > ?", rkey.ID, pivotRow.Pos).
			Order("pos ASC").Limit(1)
	} else {
		neighborQuery = tx.Where("kid = ? AND pos < ?", rkey.ID, pivotRow.Pos).
			Order("pos DESC").Limit(1)
	}
	var neighbor store.RList
	err = neighborQuery.First(&neighbor).Error
	if err == nil {
		neighborPos = &neighbor.Pos
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	if neighborPos != nil && abs64(pivotRow.Pos-*neighborPos) <= int64(len(items)) {
		if err := d.rebalanceInsertPositions(tx, rkey.ID); err != nil {
			return err
		}
		var canonical []store.RList
		if err := tx.Where("kid = ?", rkey.ID).
			Order("pos ASC").Find(&canonical).Error; err != nil {
			return err
		}
		pivotIdx := 0
		for i, row := range canonical {
			if row.ID == pivotRow.ID {
				pivotIdx = i
				break
			}
		}
		pivotRow.Pos = int64(pivotIdx * listPositionInterval)
		neighborPos = nil
		if first.after && pivotIdx+1 < len(canonical) {
			pos := int64((pivotIdx + 1) * listPositionInterval)
			neighborPos = &pos
		} else if !first.after && pivotIdx > 0 {
			pos := int64((pivotIdx - 1) * listPositionInterval)
			neighborPos = &pos
		}
	}

	rows := make([]store.RList, len(items))
	for i := range items {
		var pos int64
		if neighborPos == nil {
			step := int64(listPositionInterval / 2)
			if len(items) > 1 {
				step = int64(listPositionInterval / (2 * len(items)))
				if step == 0 {
					step = 1
				}
			}
			if first.after {
				pos = pivotRow.Pos + step*int64(i+1)
			} else {
				pos = pivotRow.Pos - step*int64(i+1)
			}
		} else if len(items) == 1 {
			pos = (*neighborPos + pivotRow.Pos) / 2
		} else if first.after {
			pos = *neighborPos - 1 - int64(i)
		} else {
			pos = *neighborPos + 1 + int64(i)
		}
		rows[i] = store.RList{KID: rkey.ID, Pos: pos, Elem: first.elem}
		*items[i].length = rkey.KLen + i + 1
	}

	if err := tx.Create(&rows).Error; err != nil {
		return err
	}
	return tx.Model(&store.RKey{}).
		Where("id = ?", rkey.ID).
		Updates(map[string]any{
			"klen": gorm.Expr("klen + ?", len(rows)),
			"klist_head": gorm.Expr(
				"(SELECT MIN(pos) FROM rlist WHERE rlist.kid = ?)", rkey.ID),
			"klist_tail": gorm.Expr(
				"(SELECT MAX(pos) FROM rlist WHERE rlist.kid = ?)", rkey.ID),
		}).Error
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

func (d *DB) rebalanceInsertPositions(tx *gorm.DB, kid int) error {
	var elems []store.RList
	if err := tx.Where("kid = ?", kid).
		Order("pos ASC").Find(&elems).Error; err != nil {
		return err
	}
	if len(elems) == 0 {
		return core.ErrNotFound
	}

	minPos := elems[0].Pos
	maxPos := elems[len(elems)-1].Pos
	tempOffset := maxPos - minPos +
		int64(len(elems))*listPositionInterval + 1
	if err := tx.Model(&store.RList{}).
		Where("kid = ?", kid).
		Update("pos", gorm.Expr("pos + ?", tempOffset)).Error; err != nil {
		return err
	}

	var bulk strings.Builder
	bulk.WriteString("UPDATE rlist SET pos = CASE id")
	for i, elem := range elems {
		bulk.WriteString(" WHEN ")
		bulk.WriteString(strconv.Itoa(elem.ID))
		bulk.WriteString(" THEN ")
		bulk.WriteString(strconv.FormatInt(int64(i*listPositionInterval), 10))
	}
	bulk.WriteString(" END WHERE id IN (")
	for i, elem := range elems {
		if i > 0 {
			bulk.WriteByte(',')
		}
		bulk.WriteString(strconv.Itoa(elem.ID))
	}
	bulk.WriteByte(')')
	return tx.Exec(bulk.String()).Error
}

func (d *DB) insertSlow(key string, pivot any, elem any, after bool) (int, error) {
	pivotb, err := core.ToBytes(pivot)
	if err != nil {
		return 0, err
	}
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var newLen int64
	err = d.store.TransactionWrite(d.ctx, d.writeScope("insert", key),
		func(tx *gorm.DB, dialect store.Dialect) error {
			var rkey store.RKey
			keyQuery := tx.Model(&store.RKey{}).
				Select("id", "klen").
				Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeList.Value())
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				keyQuery = keyQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			err := keyQuery.First(&rkey).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return core.ErrNotFound
			}
			if err != nil {
				return err
			}

			pivotQuery := tx.Model(&store.RList{}).
				Where("kid = ? AND elem = ?", rkey.ID, pivotb)
			var pivotElem store.RList
			err = pivotQuery.
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
			rebalance := func() error {
				var elems []store.RList
				if err := tx.Model(&store.RList{}).
					Where("kid = ?", rkey.ID).
					Order("pos ASC").
					Find(&elems).Error; err != nil {
					return err
				}
				if len(elems) == 0 {
					return core.ErrNotFound
				}

				// Move all rows above the current range first, then assign the
				// final positions. Updating rows directly to final positions can
				// violate the (kid, pos) unique index while earlier rows move.
				minPos := elems[0].Pos
				maxPos := elems[len(elems)-1].Pos
				tempOffset := maxPos - minPos +
					int64(len(elems))*listPositionInterval + 1
				if err := tx.Model(&store.RList{}).
					Where("kid = ?", rkey.ID).
					Update("pos", gorm.Expr("pos + ?", tempOffset)).Error; err != nil {
					return err
				}

				pivotIdx := 0
				var bulk strings.Builder
				bulk.WriteString("UPDATE rlist SET pos = CASE id")
				for i, elem := range elems {
					newListPos := int64(i * listPositionInterval)
					bulk.WriteString(" WHEN ")
					bulk.WriteString(strconv.Itoa(elem.ID))
					bulk.WriteString(" THEN ")
					bulk.WriteString(strconv.FormatInt(newListPos, 10))
					if elem.ID == pivotElem.ID {
						pivotIdx = i
					}
				}
				bulk.WriteString(" END WHERE id IN (")
				for i, elem := range elems {
					if i > 0 {
						bulk.WriteByte(',')
					}
					bulk.WriteString(strconv.Itoa(elem.ID))
				}
				bulk.WriteByte(')')
				if err := tx.Exec(bulk.String()).Error; err != nil {
					return err
				}

				if after {
					newPos = int64((pivotIdx+1)*listPositionInterval + listPositionInterval/2)
				} else {
					newPos = int64(pivotIdx*listPositionInterval - listPositionInterval/2)
				}
				return nil
			}

			if after {
				nextQuery := tx.Model(&store.RList{}).
					Where("kid = ? AND pos > ?", rkey.ID, pivotElem.Pos)
				var nextElem store.RList
				err = nextQuery.
					Order("pos ASC").
					Limit(1).
					First(&nextElem).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					newPos = pivotElem.Pos + listPositionInterval
				} else if err != nil {
					return err
				} else {
					newPos = pivotElem.Pos + (nextElem.Pos-pivotElem.Pos)/2
					if newPos == pivotElem.Pos {
						if err := rebalance(); err != nil {
							return err
						}
					}
				}
			} else {
				prevQuery := tx.Model(&store.RList{}).
					Where("kid = ? AND pos < ?", rkey.ID, pivotElem.Pos)
				var prevElem store.RList
				err = prevQuery.
					Order("pos DESC").
					Limit(1).
					First(&prevElem).Error
				if errors.Is(err, gorm.ErrRecordNotFound) {
					newPos = pivotElem.Pos - listPositionInterval
				} else if err != nil {
					return err
				} else {
					newPos = prevElem.Pos + (pivotElem.Pos-prevElem.Pos)/2
					if newPos == prevElem.Pos {
						if err := rebalance(); err != nil {
							return err
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

			if err := tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]any{
					"klen": gorm.Expr("klen + 1"),
					"klist_head": gorm.Expr(
						"(SELECT MIN(pos) FROM rlist WHERE rlist.kid = ?)", rkey.ID),
					"klist_tail": gorm.Expr(
						"(SELECT MAX(pos) FROM rlist WHERE rlist.kid = ?)", rkey.ID),
				}).Error; err != nil {
				return err
			}
			newLen = int64(rkey.KLen) + 1

			return nil
		})

	return int(newLen), err
}

func (d *DB) pushMany(key string, elems []any, back bool) (int, error) {
	newLen := 0
	pushes := make([]listPushItem, len(elems))
	for i, elem := range elems {
		elemb, err := core.ToBytes(elem)
		if err != nil {
			return 0, err
		}
		pushes[i] = listPushItem{elem: elemb, back: back, length: &newLen}
	}
	item := listPushBatch{items: pushes}
	var err error
	err = d.store.TransactionWriteItems(d.ctx,
		"list:push:"+strconv.Itoa(d.dbIdx)+"\x00"+key, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			run := func(tx *gorm.DB) error {
				now := time.Now().UnixMilli()
				rkey, err := store.LockKey(tx, dialect, d.dbIdx, key)
				if errors.Is(err, gorm.ErrRecordNotFound) {
					rkey, err = store.EnsureKeyLocked(tx, dialect, d.dbIdx,
						key, core.TypeList.Value(), now, 0)
				}
				if err != nil {
					return err
				}
				if rkey.KType != core.TypeList.Value() {
					return core.ErrKeyType
				}
				expired := rkey.ExpireAt != nil && *rkey.ExpireAt <= now

				var head, tail int64
				if rkey.KListHead != nil {
					head = *rkey.KListHead
				}
				if rkey.KListTail != nil {
					tail = *rkey.KListTail
				}
				empty := rkey.KLen == 0
				baseLen := rkey.KLen
				if expired {
					if err := tx.Where("kid = ?", rkey.ID).
						Delete(&store.RList{}).Error; err != nil {
						return err
					}
					empty = true
					baseLen = 0
				}
				if !empty && rkey.KListHead == nil {
					if err := tx.Model(&store.RList{}).
						Select("MIN(pos)").
						Where("kid = ?", rkey.ID).
						Scan(&head).Error; err != nil {
						return err
					}
				}
				if !empty && rkey.KListTail == nil {
					if err := tx.Model(&store.RList{}).
						Select("MAX(pos)").
						Where("kid = ?", rkey.ID).
						Scan(&tail).Error; err != nil {
						return err
					}
				}

				rows := make([]store.RList, 0, len(items))
				sequence := 0
				for _, raw := range items {
					batch := raw.(listPushBatch)
					for _, push := range batch.items {
						var pos int64
						if empty {
							pos = listPositionInterval
							head, tail = pos, pos
							empty = false
						} else if push.back {
							tail += listPositionInterval
							pos = tail
						} else {
							head -= listPositionInterval
							pos = head
						}
						*push.length = baseLen + sequence + 1
						sequence++
						rows = append(rows, store.RList{
							KID:  rkey.ID,
							Pos:  pos,
							Elem: push.elem,
						})
					}
				}

				if len(rows) == 1 {
					if err := tx.Create(&rows[0]).Error; err != nil {
						return err
					}
				} else if err := tx.Create(&rows).Error; err != nil {
					return err
				}

				return tx.Model(&store.RKey{}).
					Where("id = ?", rkey.ID).
					Updates(listPushUpdates(now, expired, len(rows), head, tail)).Error
			}
			if len(items) == 1 && !d.store.InTx() {
				return tx.Transaction(func(tx *gorm.DB) error {
					return run(tx)
				})
			}
			return run(tx)
		})

	return newLen, err
}

type listPushItem struct {
	elem   []byte
	back   bool
	length *int
}

type listPushBatch struct {
	items []listPushItem
}

func listPushUpdates(now int64, expired bool, count int, head, tail int64) map[string]any {
	updates := map[string]any{
		"kver":        gorm.Expr("kver + 1"),
		"modified_at": now,
		"klist_head":  head,
		"klist_tail":  tail,
	}
	if expired {
		updates["expire_at"] = nil
		updates["klen"] = count
	} else {
		updates["klen"] = gorm.Expr("klen + ?", count)
	}
	return updates
}
