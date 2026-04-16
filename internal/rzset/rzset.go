// Package rzset is a database-backed sorted set repository.
// It provides methods to interact with sorted sets in the database.
package rzset

import (
	"context"
	"errors"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
)

// SQL aggregation functions for set operations.
const (
	AggregateSum = "SUM(score)"
	AggregateMin = "MIN(score)"
	AggregateMax = "MAX(score)"
)

// SQL sort directions.
const (
	SortAsc  = "ASC"
	SortDesc = "DESC"
)

// scanPageSize is the default number
// of set items per page when scanning.
const scanPageSize = 10

// DB is a database-backed sorted set repository.
// A sorted set (zset) is a like a set, but each element has a score.
// While elements are unique, scores can be repeated.
//
// Elements in the set are ordered by score (from low to high), and then
// by lexicographical order (ascending). Adding, updating or removing
// elements maintains the order of the set.
//
// Use the sorted set repository to work with sets and their elements,
// and to perform set operations like union or intersection.
type DB struct {
	store *store.Store
	dbIdx int
}

// New connects to the sorted set repository.
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

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elem any, score float64) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}

	var created bool
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      5,
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			rzset := store.RZSet{
				KID:   rkey.ID,
				Elem:  elemb,
				Score: score,
			}
			if err := tx.Create(&rzset).Error; err != nil {
				return err
			}
			created = true
			return nil

		case err != nil:
			return err

		case rkey.KType != 5:
			return core.ErrKeyType

		default:
			var existing store.RZSet
			err = tx.Where("kid = ? AND elem = ?", rkey.ID, elemb).First(&existing).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				rzset := store.RZSet{
					KID:   rkey.ID,
					Elem:  elemb,
					Score: score,
				}
				if err := tx.Create(&rzset).Error; err != nil {
					return err
				}
				if err := tx.Model(&store.RKey{}).
					Where("id = ?", rkey.ID).
					Update("klen", gorm.Expr("klen + 1")).Error; err != nil {
					return err
				}
				created = true
			} else if err != nil {
				return err
			} else {
				existing.Score = score
				if err := tx.Save(&existing).Error; err != nil {
					return err
				}
				created = false
			}

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

// AddMany adds or updates multiple elements in a set.
func (d *DB) AddMany(key string, items map[any]float64) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	var created int
	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      5,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}

		case err != nil:
			return err

		case rkey.KType != 5:
			return core.ErrKeyType
		}

		// 转换所有元素为字节
		elems := make(map[string]float64, len(items))
		for elem, score := range items {
			elemb, err := core.ToBytes(elem)
			if err != nil {
				return err
			}
			elems[string(elemb)] = score
		}

		// 批量查询已存在的元素，避免 N+1 查询
		elemBytes := make([][]byte, 0, len(elems))
		for elemb := range elems {
			elemBytes = append(elemBytes, []byte(elemb))
		}

		var existingElems []store.RZSet
		err = tx.Where("kid = ? AND elem IN ?", rkey.ID, elemBytes).Find(&existingElems).Error
		if err != nil {
			return err
		}

		// 构建已存在元素的映射
		existingMap := make(map[string]*store.RZSet, len(existingElems))
		for i := range existingElems {
			existingMap[string(existingElems[i].Elem)] = &existingElems[i]
		}

		// 分离新增和更新的元素
		var newElems []store.RZSet
		var updateElems []store.RZSet
		created = 0

		for elemb, score := range elems {
			if existing, ok := existingMap[elemb]; ok {
				// 元素已存在，准备更新分数
				existing.Score = score
				updateElems = append(updateElems, *existing)
			} else {
				// 元素不存在，准备插入
				newElems = append(newElems, store.RZSet{
					KID:   rkey.ID,
					Elem:  []byte(elemb),
					Score: score,
				})
				created++
			}
		}

		// 批量插入新元素
		if len(newElems) > 0 {
			if err := tx.Create(&newElems).Error; err != nil {
				return err
			}
		}

		// 批量更新现有元素的分数
		if len(updateElems) > 0 {
			for i := range updateElems {
				if err := tx.Save(&updateElems[i]).Error; err != nil {
					return err
				}
			}
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", rkey.ID).
			Update("klen", gorm.Expr("klen + ?", created)).Error
	})

	return created, err
}

// Count returns the number of elements in a set with a score between min and max.
func (d *DB) Count(key string, min, max float64) (int, error) {
	now := time.Now().UnixMilli()
	var count int64
	err := d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.score BETWEEN ? AND ?", min, max).
		Count(&count).Error
	return int(count), err
}

// Delete removes elements from a set.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	elembs, err := core.ToBytesMany(elems...)
	if err != nil {
		return 0, err
	}

	var n int64

	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = 5", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		result := tx.Where("kid = ? AND elem IN ?", rkey.ID, elembs).Delete(&store.RZSet{})
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

// DeleteWith removes elements from a set with additional options.
func (d *DB) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{db: d, key: key}
}

// GetRank returns the rank and score of an element in a set.
func (d *DB) GetRank(key string, elem any) (rank int, score float64, err error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, 0, err
	}
	now := time.Now().UnixMilli()

	// Use a consistent read by performing both queries within a read-only
	// transaction context. While this doesn't provide serializable isolation,
	// it ensures both queries see the same snapshot at the database level.
	var result struct {
		Score float64
	}
	err = d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rzset.elem = ?", d.dbIdx, key, elemb).
		Scopes(store.NotExpired(now)).
		Select("rzset.score as score").
		Scan(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, core.ErrNotFound
	}
	if err != nil {
		return 0, 0, err
	}

	// Count elements with score less than the target score, or equal score
	// but lexicographically smaller element (for tie-breaking in ascending order)
	var count int64
	err = d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Where("rzset.score < ? OR (rzset.score = ? AND rzset.elem < ?)", result.Score, result.Score, elemb).
		Count(&count).Error
	if err != nil {
		return 0, 0, err
	}

	return int(count), result.Score, nil
}

// GetRankRev returns the rank and score of an element in a set (reverse order).
func (d *DB) GetRankRev(key string, elem any) (rank int, score float64, err error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, 0, err
	}
	now := time.Now().UnixMilli()

	var result struct {
		Score float64
	}
	err = d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rzset.elem = ?", d.dbIdx, key, elemb).
		Scopes(store.NotExpired(now)).
		Select("rzset.score as score").
		Scan(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, core.ErrNotFound
	}
	if err != nil {
		return 0, 0, err
	}

	var count int64
	err = d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Where("rzset.score > ? OR (rzset.score = ? AND rzset.elem > ?)", result.Score, result.Score, elemb).
		Count(&count).Error
	if err != nil {
		return 0, 0, err
	}

	return int(count), result.Score, nil
}

// GetScore returns the score of an element in a set.
func (d *DB) GetScore(key string, elem any) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()
	var result struct {
		Score float64
	}
	err = d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ? AND rzset.elem = ?", d.dbIdx, key, elemb).
		Scopes(store.NotExpired(now)).
		Select("rzset.score as score").
		First(&result).Error

	if errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, core.ErrNotFound
	}
	if err != nil {
		return 0, err
	}
	return result.Score, nil
}

// Incr increments the score of an element in a set.
func (d *DB) Incr(key string, elem any, delta float64) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}

	var newScore float64
	err = d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ?", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&rkey).Error

		switch {
		case errors.Is(err, gorm.ErrRecordNotFound):
			rkey = store.RKey{
				KDB:        d.dbIdx,
				KName:      key,
				KType:      5,
				KVer:       1,
				ModifiedAt: now,
				KLen:       1,
			}
			if err := tx.Create(&rkey).Error; err != nil {
				return err
			}
			rzset := store.RZSet{
				KID:   rkey.ID,
				Elem:  elemb,
				Score: delta,
			}
			if err := tx.Create(&rzset).Error; err != nil {
				return err
			}
			newScore = delta
			return nil

		case err != nil:
			return err

		case rkey.KType != 5:
			return core.ErrKeyType

		default:
			var existing store.RZSet
			err = tx.Where("kid = ? AND elem = ?", rkey.ID, elemb).First(&existing).Error

			if errors.Is(err, gorm.ErrRecordNotFound) {
				rzset := store.RZSet{
					KID:   rkey.ID,
					Elem:  elemb,
					Score: delta,
				}
				if err := tx.Create(&rzset).Error; err != nil {
					return err
				}
				if err := tx.Model(&store.RKey{}).
					Where("id = ?", rkey.ID).
					Update("klen", gorm.Expr("klen + 1")).Error; err != nil {
					return err
				}
				newScore = delta
			} else if err != nil {
				return err
			} else {
				newScore = existing.Score + delta
				existing.Score = newScore
				if err := tx.Save(&existing).Error; err != nil {
					return err
				}
			}

			return tx.Model(&store.RKey{}).
				Where("id = ?", rkey.ID).
				Updates(map[string]interface{}{
					"kver":        gorm.Expr("kver + 1"),
					"modified_at": now,
				}).Error
		}
	})

	return newScore, err
}

// Inter returns the intersection of multiple sets.
func (d *DB) Inter(keys ...string) ([]SetItem, error) {
	cmd := InterCmd{db: d, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// InterWith intersects multiple sets with additional options.
func (d *DB) InterWith(keys ...string) InterCmd {
	return InterCmd{db: d, keys: keys, aggregate: AggregateSum}
}

// Len returns the number of elements in a set.
func (d *DB) Len(key string) (int, error) {
	now := time.Now().UnixMilli()
	var count int64
	err := d.store.DB.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Count(&count).Error
	return int(count), err
}

// Range returns a range of elements from a set.
func (d *DB) Range(key string, start, stop int) ([]SetItem, error) {
	now := time.Now().UnixMilli()
	var rkey store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var results []store.RZSet
	err = d.store.DB.Where("kid = ?", rkey.ID).
		Order("score ASC, elem ASC").
		Offset(start).
		Limit(stop - start + 1).
		Find(&results).Error
	if err != nil {
		return nil, err
	}

	items := make([]SetItem, len(results))
	for i, r := range results {
		items[i] = SetItem{Elem: core.Value(r.Elem), Score: r.Score}
	}
	return items, nil
}

// RangeWith ranges elements from a set with additional options.
func (d *DB) RangeWith(key string) RangeCmd {
	return RangeCmd{db: d, key: key}
}

// Scan iterates over set items with elements matching pattern.
// Uses score-based cursor to ensure consistent ordering per Redis spec.
// Cursor encodes the last element's score and ID for correct pagination.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count == 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()
	var rkey store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ?", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&rkey).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return ScanResult{Cursor: 0, Items: []SetItem{}}, nil
	}
	if err != nil {
		return ScanResult{}, err
	}

	var results []store.RZSet

	// Query in score order (correct Redis semantics)
	query := d.store.DB.Where("kid = ?", rkey.ID).
		Order("score ASC, elem ASC, id ASC").
		Limit(count)

	// Decode cursor and get the last item's score from DB
	if cursor > 0 {
		// Query the score of the cursor ID
		var lastItem store.RZSet
		err := d.store.DB.Select("score").
			Where("kid = ? AND id = ?", rkey.ID, cursor).
			First(&lastItem).Error

		if err == nil {
			// Use the actual score from DB for correct filtering
			lastScore := lastItem.Score
			// Get elements after the cursor position:
			// (score > lastScore) OR (score = lastScore AND id > cursorID)
			query = query.Where(
				"score > ? OR (score = ? AND id > ?)",
				lastScore, lastScore, cursor,
			)
		} else {
			// Fallback: just use ID if score query fails
			query = query.Where("id > ?", cursor)
		}
	}

	err = query.Find(&results).Error
	if err != nil {
		return ScanResult{}, err
	}

	// Convert results
	items := make([]SetItem, len(results))
	var nextCursor int

	if len(results) > 0 {
		lastItem := results[len(results)-1]
		// Cursor is just the ID for simplicity
		nextCursor = lastItem.ID
	}

	for i, r := range results {
		items[i] = SetItem{Elem: core.Value(r.Elem), Score: r.Score}
	}

	// If we got fewer results than requested, we've reached the end
	if len(results) < count {
		nextCursor = 0
	}

	return ScanResult{
		Cursor: nextCursor,
		Items:  items,
	}, nil
}

// Union returns the union of multiple sets.
func (d *DB) Union(keys ...string) ([]SetItem, error) {
	cmd := UnionCmd{db: d, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// UnionWith unions multiple sets with additional options.
func (d *DB) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{db: d, keys: keys, aggregate: AggregateSum}
}

// DeleteByRank removes elements by rank (position).
func (d *DB) DeleteByRank(key string, start, stop int) (int, error) {
	var n int64

	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = 5", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&keyMeta).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var elemsToDelete []struct{ Elem []byte }
		err = tx.Model(&store.RZSet{}).
			Select("elem").
			Where("kid = ?", keyMeta.ID).
			Order("score ASC, elem ASC").
			Offset(start - 1).
			Limit(stop - start + 1).
			Find(&elemsToDelete).Error
		if err != nil {
			return err
		}

		if len(elemsToDelete) == 0 {
			return nil
		}

		elemVals := make([][]byte, len(elemsToDelete))
		for i, e := range elemsToDelete {
			elemVals[i] = e.Elem
		}

		result := tx.Where("kid = ? AND elem IN ?", keyMeta.ID, elemVals).Delete(&store.RZSet{})
		if result.Error != nil {
			return result.Error
		}

		n = result.RowsAffected
		if n > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
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

// DeleteByScore removes elements by score range.
func (d *DB) DeleteByScore(key string, min, max float64) (int, error) {
	var n int64

	err := d.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = 5", d.dbIdx, key).
			Scopes(store.NotExpired(now)).
			First(&keyMeta).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		result := tx.Where("kid = ? AND score BETWEEN ? AND ?", keyMeta.ID, min, max).Delete(&store.RZSet{})
		if result.Error != nil {
			return result.Error
		}

		n = result.RowsAffected
		if n > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
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

// GetRange returns elements by rank (position).
func (d *DB) GetRange(key string, start, stop int) ([]core.Value, []float64, error) {
	now := time.Now().UnixMilli()

	var keyMeta store.RKey
	err := d.store.DB.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", d.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	offset := start - 1
	limit := stop - start + 1
	if limit <= 0 {
		return nil, nil, nil
	}

	var results []struct {
		Elem  []byte
		Score float64
	}
	err = d.store.DB.Model(&store.RZSet{}).
		Select("elem, score").
		Where("kid = ?", keyMeta.ID).
		Order("score ASC, elem ASC").
		Offset(offset).
		Limit(limit).
		Find(&results).Error
	if err != nil {
		return nil, nil, err
	}

	elems := make([]core.Value, len(results))
	scores := make([]float64, len(results))
	for i, r := range results {
		elems[i] = core.Value(r.Elem)
		scores[i] = r.Score
	}
	return elems, scores, nil
}

// GetRangeByScore returns elements with scores in a given range.
func (d *DB) GetRangeByScore(key string, min, max float64) ([]core.Value, []float64, error) {
	now := time.Now().UnixMilli()

	var results []struct {
		Elem  []byte
		Score float64
	}
	err := d.store.DB.Model(&store.RZSet{}).
		Select("rzset.elem as elem, rzset.score as score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.score BETWEEN ? AND ?", min, max).
		Order("rzset.score ASC, rzset.elem ASC").
		Find(&results).Error
	if err != nil {
		return nil, nil, err
	}

	elems := make([]core.Value, len(results))
	scores := make([]float64, len(results))
	for i, r := range results {
		elems[i] = core.Value(r.Elem)
		scores[i] = r.Score
	}
	return elems, scores, nil
}

// byRank is used for rank-based operations.
type byRank struct {
	start, stop int
}

// byScore is used for score-based operations.
type byScore struct {
	start, stop float64
}

// RangeCmd retrieves a range of elements from a sorted set.
type RangeCmd struct {
	db      *DB
	key     string
	byRank  *byRank
	byScore *byScore
	sortDir string
	offset  int
	count   int
}

// ByRank sets filtering and sorting by rank.
// Start and stop are 0-based, inclusive.
// Negative values are not supported.
func (c RangeCmd) ByRank(start, stop int) RangeCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering and sorting by score.
// Start and stop are inclusive.
func (c RangeCmd) ByScore(start, stop float64) RangeCmd {
	c.byScore = &byScore{start, stop}
	c.byRank = nil
	return c
}

// Asc sets the sorting direction to ascending.
func (c RangeCmd) Asc() RangeCmd {
	c.sortDir = SortAsc
	return c
}

// Desc sets the sorting direction to descending.
func (c RangeCmd) Desc() RangeCmd {
	c.sortDir = SortDesc
	return c
}

// Offset sets the offset of the range.
// Only takes effect when filtering by score.
func (c RangeCmd) Offset(offset int) RangeCmd {
	c.offset = offset
	return c
}

// Count sets the maximum number of elements to return.
// Only takes effect when filtering by score.
func (c RangeCmd) Count(count int) RangeCmd {
	c.count = count
	return c
}

// Run returns a range of elements from a sorted set.
// Uses either by-rank or by-score filtering. The elements are sorted
// by score/rank and then by element according to the sorting direction.
//
// Offset and count are optional, and only take effect
// when filtering by score.
//
// If the key does not exist or is not a sorted set,
// returns a nil slice.
func (c RangeCmd) Run() ([]SetItem, error) {
	if c.byRank != nil {
		return c.rangeRank()
	}
	if c.byScore != nil {
		return c.rangeScore()
	}
	return nil, nil
}

// rangeRank retrieves a range of elements by rank.
func (c RangeCmd) rangeRank() ([]SetItem, error) {
	// Check start and stop values.
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return nil, nil
	}

	now := time.Now().UnixMilli()

	// Get database and dbIdx
	var db *gorm.DB
	var dbIdx int
	if c.db != nil {
		db = c.db.store.DB
		dbIdx = c.db.dbIdx
	} else {
		return nil, errors.New("no database available")
	}

	// Get the key ID
	var keyMeta store.RKey
	err := db.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Calculate limit
	count := c.byRank.stop - c.byRank.start + 1
	if count <= 0 {
		return nil, nil
	}

	// Determine order
	order := "score ASC, elem ASC"
	if c.sortDir == SortDesc {
		order = "score DESC, elem DESC"
	}

	var items []SetItem
	err = db.Model(&store.RZSet{}).
		Select("elem, score").
		Where("kid = ?", keyMeta.ID).
		Order(order).
		Offset(c.byRank.start).
		Limit(count).
		Scan(&items).Error

	return items, err
}

// rangeScore retrieves a range of elements by score.
func (c RangeCmd) rangeScore() ([]SetItem, error) {
	now := time.Now().UnixMilli()

	// Get database and dbIdx
	var db *gorm.DB
	var dbIdx int
	if c.db != nil {
		db = c.db.store.DB
		dbIdx = c.db.dbIdx
	} else {
		return nil, errors.New("no database available")
	}

	// Get the key ID
	var keyMeta store.RKey
	err := db.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Determine order
	order := "score ASC, elem ASC"
	if c.sortDir == SortDesc {
		order = "score DESC, elem DESC"
	}

	query := db.Model(&store.RZSet{}).
		Select("elem, score").
		Where("kid = ?", keyMeta.ID).
		Where("score BETWEEN ? AND ?", c.byScore.start, c.byScore.stop).
		Order(order)

	// Add offset and count if necessary.
	if c.offset > 0 {
		query = query.Offset(c.offset)
	}
	if c.count > 0 {
		query = query.Limit(c.count)
	}

	var items []SetItem
	err = query.Scan(&items).Error
	return items, err
}

// DeleteCmd removes elements from a set.
type DeleteCmd struct {
	db      *DB
	key     string
	byRank  *byRank
	byScore *byScore
}

// ByRank sets filtering by rank.
// The rank is the 0-based position of the element in the set, ordered
// by score (from high to low), and then by lexicographical order (descending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
func (c DeleteCmd) ByRank(start, stop int) DeleteCmd {
	c.byRank = &byRank{start, stop}
	c.byScore = nil
	return c
}

// ByScore sets filtering by score.
// Start and stop are inclusive.
func (c DeleteCmd) ByScore(start, stop float64) DeleteCmd {
	c.byScore = &byScore{start, stop}
	c.byRank = nil
	return c
}

// Run removes elements from a set according to the
// specified criteria (rank or score range).
// Returns the number of elements removed.
// Does nothing if the key does not exist or is not a set.
func (c DeleteCmd) Run() (int, error) {
	now := time.Now().UnixMilli()
	n := 0
	var err error
	if c.byRank != nil {
		n, err = c.deleteRank(now)
	} else if c.byScore != nil {
		n, err = c.deleteScore(now)
	} else {
		return 0, nil
	}
	return n, err
}

// deleteRank removes elements from a set by rank.
// Uses transaction with row-level locking to prevent race conditions.
func (c DeleteCmd) deleteRank(now int64) (int, error) {
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return 0, nil
	}

	var n int64
	err := c.db.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = 5", c.db.dbIdx, c.key).
			Scopes(store.NotExpired(now)).
			First(&keyMeta).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		var elemsToDelete []struct{ Elem []byte }
		err = tx.Model(&store.RZSet{}).
			Select("elem").
			Where("kid = ?", keyMeta.ID).
			Order("score ASC, elem ASC").
			Offset(c.byRank.start).
			Limit(c.byRank.stop - c.byRank.start + 1).
			Find(&elemsToDelete).Error
		if err != nil {
			return err
		}

		if len(elemsToDelete) == 0 {
			return nil
		}

		elemVals := make([][]byte, len(elemsToDelete))
		for i, e := range elemsToDelete {
			elemVals[i] = e.Elem
		}

		result := tx.Where("kid = ? AND elem IN ?", keyMeta.ID, elemVals).Delete(&store.RZSet{})
		if result.Error != nil {
			return result.Error
		}

		n = result.RowsAffected
		if n > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
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

// deleteScore removes elements from a set by score.
// Uses transaction with row-level locking to prevent race conditions.
func (c DeleteCmd) deleteScore(now int64) (int, error) {
	var n int64
	err := c.db.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = 5", c.db.dbIdx, c.key).
			Scopes(store.NotExpired(now)).
			First(&keyMeta).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		result := tx.Where("kid = ? AND score BETWEEN ? AND ?", keyMeta.ID, c.byScore.start, c.byScore.stop).Delete(&store.RZSet{})
		if result.Error != nil {
			return result.Error
		}

		n = result.RowsAffected
		if n > 0 {
			return tx.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
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

// InterCmd intersects multiple sets.
type InterCmd struct {
	db        *DB
	dest      string
	keys      []string
	aggregate string
}

// Dest sets the key to store the result of the intersection.
func (c InterCmd) Dest(dest string) InterCmd {
	c.dest = dest
	return c
}

// Sum changes the aggregation function to take the sum of scores.
func (c InterCmd) Sum() InterCmd {
	c.aggregate = AggregateSum
	return c
}

// Min changes the aggregation function to take the minimum score.
func (c InterCmd) Min() InterCmd {
	c.aggregate = AggregateMin
	return c
}

// Max changes the aggregation function to take the maximum score.
func (c InterCmd) Max() InterCmd {
	c.aggregate = AggregateMax
	return c
}

// Run returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// The score of each element is the aggregate of its scores in the given sets.
// If any of the source keys do not exist or are not sets, returns an empty slice.
// Uses the default database connection (not part of any transaction).
func (c InterCmd) Run() ([]SetItem, error) {
	return c.runTx(c.db.store.DB)
}

// runTx returns the intersection of multiple sets within a transaction.
// Used internally by Store() to ensure atomicity.
func (c InterCmd) runTx(db *gorm.DB) ([]SetItem, error) {
	now := time.Now().UnixMilli()

	var keyIDs []int
	err := db.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = 5", c.db.dbIdx, c.keys).
		Scopes(store.NotExpired(now)).
		Pluck("id", &keyIDs).Error
	if err != nil {
		return nil, err
	}

	if len(keyIDs) != len(c.keys) {
		return nil, nil
	}

	aggExpr := "SUM(score) as score"
	switch c.aggregate {
	case AggregateMin:
		aggExpr = "MIN(score) as score"
	case AggregateMax:
		aggExpr = "MAX(score) as score"
	}

	var items []SetItem
	err = db.Model(&store.RZSet{}).
		Select("elem, "+aggExpr).
		Where("kid IN ?", keyIDs).
		Group("elem").
		Having("COUNT(DISTINCT kid) = ?", len(c.keys)).
		Scan(&items).Error

	return items, err
}

// Store intersects multiple sets and stores the result in a new set.
// The intersection calculation and storage are performed within the same
// transaction to ensure atomicity.
func (c InterCmd) Store() (int, error) {
	now := time.Now().UnixMilli()

	var resultCount int
	err := c.db.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		// Calculate intersection within the same transaction for atomicity
		items, err := c.runTx(tx)
		if err != nil {
			return err
		}

		var destKey store.RKey
		err = tx.Where("kdb = ? AND kname = ?", c.db.dbIdx, c.dest).First(&destKey).Error
		switch {
		case err == nil:
			if destKey.KType != 5 {
				return core.ErrKeyType
			}
			if err := tx.Where("kid = ?", destKey.ID).Delete(&store.RZSet{}).Error; err != nil {
				return err
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			destKey = store.RKey{
				KDB:        c.db.dbIdx,
				KName:      c.dest,
				KType:      5,
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

		if len(items) == 0 {
			return tx.Where("kid = ?", destKey.ID).Delete(&store.RZSet{}).Error
		}

		resultCount = len(items)
		rzsets := make([]store.RZSet, len(items))
		for i, item := range items {
			rzsets[i] = store.RZSet{
				KID:   destKey.ID,
				Elem:  []byte(item.Elem),
				Score: item.Score,
			}
		}
		if err := tx.Create(&rzsets).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        len(items),
			}).Error
	})

	return resultCount, err
}

// UnionCmd unions multiple sets.
type UnionCmd struct {
	db        *DB
	dest      string
	keys      []string
	aggregate string
}

// Dest sets the key to store the result of the union.
func (c UnionCmd) Dest(dest string) UnionCmd {
	c.dest = dest
	return c
}

// Sum changes the aggregation function to take the sum of scores.
func (c UnionCmd) Sum() UnionCmd {
	c.aggregate = AggregateSum
	return c
}

// Min changes the aggregation function to take the minimum score.
func (c UnionCmd) Min() UnionCmd {
	c.aggregate = AggregateMin
	return c
}

// Max changes the aggregation function to take the maximum score.
func (c UnionCmd) Max() UnionCmd {
	c.aggregate = AggregateMax
	return c
}

// Run returns the union of multiple sets.
// The union consists of elements that exist in any of the given sets.
// The score of each element is the aggregate of its scores in the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns a nil slice.
// Uses the default database connection (not part of any transaction).
func (c UnionCmd) Run() ([]SetItem, error) {
	return c.runTx(c.db.store.DB)
}

// runTx returns the union of multiple sets within a transaction.
// Used internally by Store() to ensure atomicity.
func (c UnionCmd) runTx(db *gorm.DB) ([]SetItem, error) {
	now := time.Now().UnixMilli()

	var keyIDs []int
	err := db.Model(&store.RKey{}).
		Where("kdb = ? AND kname IN ? AND ktype = 5", c.db.dbIdx, c.keys).
		Scopes(store.NotExpired(now)).
		Pluck("id", &keyIDs).Error
	if err != nil {
		return nil, err
	}

	if len(keyIDs) == 0 {
		return nil, nil
	}

	aggExpr := "SUM(score) as score"
	switch c.aggregate {
	case AggregateMin:
		aggExpr = "MIN(score) as score"
	case AggregateMax:
		aggExpr = "MAX(score) as score"
	}

	var items []SetItem
	err = db.Model(&store.RZSet{}).
		Select("elem, "+aggExpr).
		Where("kid IN ?", keyIDs).
		Group("elem").
		Scan(&items).Error

	return items, err
}

// Store unions multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// The union calculation and storage are performed within the same
// transaction to ensure atomicity.
func (c UnionCmd) Store() (int, error) {
	now := time.Now().UnixMilli()

	var resultCount int
	err := c.db.store.Transaction(context.Background(), func(tx *gorm.DB, _ store.Dialect) error {
		// Calculate union within the same transaction for atomicity
		items, err := c.runTx(tx)
		if err != nil {
			return err
		}

		if len(items) == 0 {
			return tx.Where("kdb = ? AND kname = ?", c.db.dbIdx, c.dest).Delete(&store.RKey{}).Error
		}

		var destKey store.RKey
		err = tx.Where("kdb = ? AND kname = ?", c.db.dbIdx, c.dest).First(&destKey).Error
		switch {
		case err == nil:
			if destKey.KType != 5 {
				return core.ErrKeyType
			}
			if err := tx.Where("kid = ?", destKey.ID).Delete(&store.RZSet{}).Error; err != nil {
				return err
			}
		case errors.Is(err, gorm.ErrRecordNotFound):
			destKey = store.RKey{
				KDB:        c.db.dbIdx,
				KName:      c.dest,
				KType:      5,
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

		resultCount = len(items)
		rzsets := make([]store.RZSet, len(items))
		for i, item := range items {
			rzsets[i] = store.RZSet{
				KID:   destKey.ID,
				Elem:  []byte(item.Elem),
				Score: item.Score,
			}
		}
		if err := tx.Create(&rzsets).Error; err != nil {
			return err
		}

		return tx.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        len(items),
			}).Error
	})

	return resultCount, err
}
