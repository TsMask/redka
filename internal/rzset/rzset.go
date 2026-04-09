// Package rzset is a database-backed sorted set repository.
// It provides methods to interact with sorted sets in the database.
package rzset

import (
	"context"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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
	store  *store.Store
	update func(f func(tx *Tx) error) error
	dbIdx  int
}

// New connects to the sorted set repository.
// Does not create the database schema.
func New(s *store.Store) *DB {
	newTxFn := func(dialect store.Dialect, tx *gorm.DB, ctx context.Context) *Tx {
		return NewTx(dialect, tx, store.CtxDBIdx(ctx))
	}
	actor := store.NewTransactor(s, newTxFn)
	return &DB{store: s, update: actor.Update, dbIdx: 0}
}

// WithDB changes the logical database index in place and returns the same DB.
// It is safe for concurrent use; each TCP connection has its own DB instance.
func (d *DB) WithDB(dbIdx int) *DB {
	d.dbIdx = dbIdx
	return d
}

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elem any, score float64) (bool, error) {
	var created bool
	err := d.update(func(tx *Tx) error {
		var err error
		created, err = tx.Add(key, elem, score)
		return err
	})
	return created, err
}

// AddMany adds or updates multiple elements in a set.
// Returns the number of elements created (as opposed to updated).
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) AddMany(key string, items map[any]float64) (int, error) {
	var count int
	err := d.update(func(tx *Tx) error {
		var err error
		count, err = tx.AddMany(key, items)
		return err
	})
	return count, err
}

// Count returns the number of elements in a set with a score between
// min and max (inclusive). Exclusive ranges are not supported.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Count(key string, min, max float64) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Count(key, min, max)
}

// Delete removes elements from a set.
// Returns the number of elements removed.
// Ignores the elements that do not exist.
// Does nothing if the key does not exist or is not a set.
func (d *DB) Delete(key string, elems ...any) (int, error) {
	var n int
	err := d.update(func(tx *Tx) error {
		var err error
		n, err = tx.Delete(key, elems...)
		return err
	})
	return n, err
}

// DeleteWith removes elements from a set with additional options.
func (d *DB) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{db: d, key: key}
}

// GetRank returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetRank(key string, elem any) (rank int, score float64, err error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetRank(key, elem)
}

// GetRankRev returns the rank and score of an element in a set.
// The rank is the 0-based position of the element in the set, ordered
// by score (from high to low), and then by lexicographical order (descending).
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetRankRev(key string, elem any) (rank int, score float64, err error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetRankRev(key, elem)
}

// GetScore returns the score of an element in a set.
// If the element does not exist, returns ErrNotFound.
// If the key does not exist or is not a set, returns ErrNotFound.
func (d *DB) GetScore(key string, elem any) (float64, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.GetScore(key, elem)
}

// Incr increments the score of an element in a set.
// Returns the score after the increment.
// If the element does not exist, adds it and sets the score to 0.0
// before the increment. If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Incr(key string, elem any, delta float64) (float64, error) {
	var score float64
	err := d.update(func(tx *Tx) error {
		var err error
		score, err = tx.Incr(key, elem, delta)
		return err
	})
	return score, err
}

// Inter returns the intersection of multiple sets.
// The intersection consists of elements that exist in all given sets.
// The score of each element is the sum of its scores in the given sets.
// If any of the source keys do not exist or are not sets, returns an empty slice.
func (d *DB) Inter(keys ...string) ([]SetItem, error) {
	cmd := InterCmd{db: d, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// InterWith intersects multiple sets with additional options.
func (d *DB) InterWith(keys ...string) InterCmd {
	return InterCmd{db: d, keys: keys, aggregate: AggregateSum}
}

// Len returns the number of elements in a set.
// Returns 0 if the key does not exist or is not a set.
func (d *DB) Len(key string) (int, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Len(key)
}

// Range returns a range of elements from a set with ranks between start and stop.
// The rank is the 0-based position of the element in the set, ordered
// by score (from low to high), and then by lexicographical order (ascending).
// Start and stop are 0-based, inclusive. Negative values are not supported.
// If the key does not exist or is not a set, returns a nil slice.
func (d *DB) Range(key string, start, stop int) ([]SetItem, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Range(key, start, stop)
}

// RangeWith ranges elements from a set with additional options.
func (d *DB) RangeWith(key string) RangeCmd {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.RangeWith(key)
}

// Scan iterates over set items with elements matching pattern.
// Returns a slice of element-score pairs (see [SetItem]) of size count
// based on the current state of the cursor. Returns an empty SetItem
// slice when there are no more items.
// If the key does not exist or is not a set, returns a nil slice.
// Supports glob-style patterns. Set count = 0 for default page size.
func (d *DB) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	tx := NewTx(d.store.Dialect, d.store.DB, d.dbIdx)
	return tx.Scan(key, cursor, pattern, count)
}

// Scanner returns an iterator for set items with elements matching pattern.
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
// The score of each element is the sum of its scores in the given sets.
// Ignores the keys that do not exist or are not sets.
// If no keys exist, returns a nil slice.
func (d *DB) Union(keys ...string) ([]SetItem, error) {
	cmd := UnionCmd{db: d, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// UnionWith unions multiple sets with additional options.
func (d *DB) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{db: d, keys: keys, aggregate: AggregateSum}
}

// Tx is a sorted set repository transaction.
type Tx struct {
	dialect store.Dialect
	tx      *gorm.DB
	dbIdx   int
}

// NewTx creates a sorted set repository transaction
// from a generic database transaction.
func NewTx(dialect store.Dialect, tx *gorm.DB, dbIdx int) *Tx {
	return &Tx{dialect: dialect, tx: tx, dbIdx: dbIdx}
}

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
func (tx *Tx) Add(key string, elem any, score float64) (bool, error) {
	existCount, err := tx.count(key, elem)
	if err != nil {
		return false, err
	}
	err = tx.add(key, elem, score)
	if err != nil {
		return false, err
	}
	return existCount == 0, nil
}

// AddMany adds or updates multiple elements in a set.
// Returns the number of elements created (as opposed to updated).
func (tx *Tx) AddMany(key string, items map[any]float64) (int, error) {
	elems := make([]any, 0, len(items))
	for elem := range items {
		elems = append(elems, elem)
	}

	existCount, err := tx.count(key, elems...)
	if err != nil {
		return 0, err
	}

	for elem, score := range items {
		err := tx.add(key, elem, score)
		if err != nil {
			return 0, err
		}
	}

	return len(items) - existCount, nil
}

// Count returns the number of elements in a set with a score between
// min and max (inclusive).
func (tx *Tx) Count(key string, min, max float64) (int, error) {
	now := time.Now().UnixMilli()

	var count int64
	err := tx.tx.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.score BETWEEN ? AND ?", min, max).
		Count(&count).Error
	return int(count), err
}

// Delete removes elements from a set.
// Returns the number of elements deleted.
func (tx *Tx) Delete(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	now := time.Now().UnixMilli()

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Convert elems to bytes
	elemsBytes := make([][]byte, len(elems))
	for i, elem := range elems {
		b, err := core.ToBytes(elem)
		if err != nil {
			return 0, err
		}
		elemsBytes[i] = b
	}

	// Delete elements
	result := tx.tx.Where("kid = ? AND elem IN ?", keyMeta.ID, elemsBytes).Delete(&store.RZSet{})
	if result.Error != nil {
		return 0, result.Error
	}

	n := result.RowsAffected
	if n > 0 {
		// Update key metadata
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", keyMeta.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", n),
			})
	}

	return int(n), nil
}

// DeleteByRank removes elements by rank (position).
// Start and stop are 1-based indexes.
// Returns the number of elements deleted.
func (tx *Tx) DeleteByRank(key string, start, stop int) (int, error) {
	now := time.Now().UnixMilli()

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Get elements to delete by rank using GORM Offset/Limit
	var elemsToDelete []struct{ Elem []byte }
	err = tx.tx.Model(&store.RZSet{}).
		Select("elem").
		Where("kid = ?", keyMeta.ID).
		Order("score ASC, elem ASC").
		Offset(start - 1).
		Limit(stop - start + 1).
		Find(&elemsToDelete).Error
	if err != nil {
		return 0, err
	}

	if len(elemsToDelete) == 0 {
		return 0, nil
	}

	// Collect element values for batch delete
	elemVals := make([][]byte, len(elemsToDelete))
	for i, e := range elemsToDelete {
		elemVals[i] = e.Elem
	}

	// Delete elements using the collected element values
	result := tx.tx.Where("kid = ? AND elem IN ?", keyMeta.ID, elemVals).Delete(&store.RZSet{})
	if result.Error != nil {
		return 0, result.Error
	}

	n := result.RowsAffected
	if n > 0 {
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", keyMeta.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", n),
			})
	}

	return int(n), nil
}

// DeleteByScore removes elements by score range.
// Returns the number of elements deleted.
func (tx *Tx) DeleteByScore(key string, min, max float64) (int, error) {
	now := time.Now().UnixMilli()

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Delete elements by score range
	result := tx.tx.Where("kid = ? AND score BETWEEN ? AND ?", keyMeta.ID, min, max).Delete(&store.RZSet{})
	if result.Error != nil {
		return 0, result.Error
	}

	n := result.RowsAffected
	if n > 0 {
		tx.tx.Model(&store.RKey{}).
			Where("id = ?", keyMeta.ID).
			Updates(map[string]any{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        gorm.Expr("klen - ?", n),
			})
	}

	return int(n), nil
}

// GetRange returns elements by rank (position).
// Start and stop are 1-based indexes.
func (tx *Tx) GetRange(key string, start, stop int) ([]core.Value, []float64, error) {
	now := time.Now().UnixMilli()

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}

	// Calculate offset and limit (start and stop are 1-based)
	offset := start - 1
	limit := stop - start + 1
	if limit <= 0 {
		return nil, nil, nil
	}

	var results []struct {
		Elem  []byte
		Score float64
	}
	err = tx.tx.Model(&store.RZSet{}).
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
func (tx *Tx) GetRangeByScore(key string, min, max float64) ([]core.Value, []float64, error) {
	now := time.Now().UnixMilli()

	var results []struct {
		Elem  []byte
		Score float64
	}
	err := tx.tx.Model(&store.RZSet{}).
		Select("rzset.elem as elem, rzset.score as score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
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

// GetRank returns the rank and score of an element.
// Rank is 0-based. Returns -1 if not found.
func (tx *Tx) GetRank(key string, elem any) (int, float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return -1, 0, err
	}
	now := time.Now().UnixMilli()

	// First get the score of the element
	var score float64
	err = tx.tx.Model(&store.RZSet{}).
		Select("rzset.score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.elem = ?", elemb).
		Scan(&score).Error
	if err == gorm.ErrRecordNotFound {
		return -1, 0, core.ErrNotFound
	}
	if err != nil {
		return -1, 0, err
	}

	// Count elements with lower score, or same score but lower element value
	var rank int64
	err = tx.tx.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("(rzset.score < ? OR (rzset.score = ? AND rzset.elem < ?))", score, score, elemb).
		Count(&rank).Error
	if err != nil {
		return -1, 0, err
	}

	return int(rank), score, nil
}

// GetScore returns the score of an element.
func (tx *Tx) GetScore(key string, elem any) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	var score float64
	err = tx.tx.Model(&store.RZSet{}).
		Select("rzset.score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.elem = ?", elemb).
		Scan(&score).Error

	if err == gorm.ErrRecordNotFound {
		return 0, core.ErrNotFound
	}
	return score, err
}

// Incr increments the score of an element.
// If the element does not exist, adds it with the incremented score.
func (tx *Tx) Incr(key string, elem any, delta float64) (float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return 0, err
	}
	now := time.Now().UnixMilli()

	var newScore float64
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create the key with type checking
		var keyMeta store.RKey
		err := txInner.Where("kname = ?", key).First(&keyMeta).Error
		switch err {
		case nil:
			// Key exists, check type
			if keyMeta.KType != 5 {
				return core.ErrKeyType
			}
			// Update version and mtime
			txInner.Model(&keyMeta).Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			})
		case gorm.ErrRecordNotFound:
			// Create new key
			keyMeta = store.RKey{
				KName:      key,
				KType:      5,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&keyMeta).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Get the key ID (for newly created key)
		if keyMeta.ID == 0 {
			err = txInner.Model(&store.RKey{}).
				Where("kname = ?", key).
				First(&keyMeta).Error
			if err != nil {
				return err
			}
		}

		// Try to update existing score
		result := txInner.Model(&store.RZSet{}).
			Where("kid = ? AND elem = ?", keyMeta.ID, elemb).
			UpdateColumn("score", gorm.Expr("score + ?", delta))
		if result.Error != nil {
			return result.Error
		}

		if result.RowsAffected == 0 {
			// Element doesn't exist, insert it
			newScore = delta
			err = txInner.Create(&store.RZSet{
				KID:   keyMeta.ID,
				Elem:  elemb,
				Score: newScore,
			}).Error
			if err != nil {
				return err
			}
			// Update key length
			txInner.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
				UpdateColumn("klen", gorm.Expr("klen + 1"))
		} else {
			// Get the new score
			txInner.Model(&store.RZSet{}).
				Select("score").
				Where("kid = ? AND elem = ?", keyMeta.ID, elemb).
				Scan(&newScore)
		}

		return nil
	})

	return newScore, err
}

// Len returns the number of elements in a set.
func (tx *Tx) Len(key string) (int, error) {
	now := time.Now().UnixMilli()

	var length int64
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		Select("klen").
		Scan(&length).Error

	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	return int(length), err
}

// Scan iterates over set elements matching pattern.
// Returns a cursor for the next page and the items.
func (tx *Tx) Scan(key string, cursor int, pattern string, count int) (ScanResult, error) {
	if count <= 0 {
		count = scanPageSize
	}

	now := time.Now().UnixMilli()

	// Get the key ID first
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return ScanResult{}, nil
	}
	if err != nil {
		return ScanResult{}, err
	}

	// Build the query with glob pattern using GORM Scopes
	var results []struct {
		ID    int
		Elem  []byte
		Score float64
	}

	if tx.dialect == store.DialectSQLite {
		err = tx.tx.Model(&store.RZSet{}).
			Where("kid = ? AND id > ? AND elem GLOB ?", keyMeta.ID, cursor, pattern).
			Select("id, elem, score").
			Order("id ASC").
			Limit(count).
			Find(&results).Error
	} else {
		err = tx.tx.Model(&store.RZSet{}).
			Where("kid = ? AND id > ?", keyMeta.ID, cursor).
			Where(store.ElemPattern(tx.dialect, "elem", pattern)).
			Select("id, elem, score").
			Order("id ASC").
			Limit(count).
			Find(&results).Error
	}
	if err != nil {
		return ScanResult{}, err
	}

	items := make([]SetItem, len(results))
	var nextCursor int

	for i, r := range results {
		items[i] = SetItem{Elem: core.Value(r.Elem), Score: r.Score}
	}
	if len(results) > 0 {
		nextCursor = results[len(results)-1].ID
	}

	if nextCursor == 0 {
		return ScanResult{}, nil
	}
	return ScanResult{Cursor: nextCursor, Items: items}, nil
}

// GetRankRev returns the rank and score of an element in reverse order.
func (tx *Tx) GetRankRev(key string, elem any) (int, float64, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return -1, 0, err
	}
	now := time.Now().UnixMilli()

	// First get the score of the element
	var score float64
	err = tx.tx.Model(&store.RZSet{}).
		Select("rzset.score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.elem = ?", elemb).
		Scan(&score).Error
	if err == gorm.ErrRecordNotFound {
		return -1, 0, core.ErrNotFound
	}
	if err != nil {
		return -1, 0, err
	}

	// Count elements with higher score, or same score but higher element value
	var rank int64
	err = tx.tx.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("(rzset.score > ? OR (rzset.score = ? AND rzset.elem > ?))", score, score, elemb).
		Count(&rank).Error
	if err != nil {
		return -1, 0, err
	}

	return int(rank), score, nil
}

// Range returns elements by rank (position).
func (tx *Tx) Range(key string, start, stop int) ([]SetItem, error) {
	now := time.Now().UnixMilli()

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Calculate limit
	count := stop - start + 1
	if count <= 0 {
		return nil, nil
	}

	var items []SetItem
	err = tx.tx.Model(&store.RZSet{}).
		Select("elem, score").
		Where("kid = ?", keyMeta.ID).
		Order("score ASC, elem ASC").
		Offset(start).
		Limit(count).
		Scan(&items).Error

	return items, err
}

// RangeWith returns a command builder for range queries.
func (tx *Tx) RangeWith(key string) RangeCmd {
	return RangeCmd{tx: tx, key: key}
}

// DeleteWith removes elements from a set with additional options.
func (tx *Tx) DeleteWith(key string) DeleteCmd {
	return DeleteCmd{tx: tx, key: key}
}

// Scanner returns an iterator for set items with elements matching pattern.
func (tx *Tx) Scanner(key string, pattern string, pageSize int) *Scanner {
	return newScanner(tx, key, pattern, pageSize)
}

// Inter returns the intersection of multiple sets.
func (tx *Tx) Inter(keys ...string) ([]SetItem, error) {
	cmd := InterCmd{tx: tx, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// InterWith intersects multiple sets with additional options.
func (tx *Tx) InterWith(keys ...string) InterCmd {
	return InterCmd{tx: tx, keys: keys, aggregate: AggregateSum}
}

// Union returns the union of multiple sets.
func (tx *Tx) Union(keys ...string) ([]SetItem, error) {
	cmd := UnionCmd{tx: tx, keys: keys, aggregate: AggregateSum}
	return cmd.Run()
}

// UnionWith unions multiple sets with additional options.
func (tx *Tx) UnionWith(keys ...string) UnionCmd {
	return UnionCmd{tx: tx, keys: keys, aggregate: AggregateSum}
}

// Helper methods

func (tx *Tx) count(key string, elems ...any) (int, error) {
	if len(elems) == 0 {
		return 0, nil
	}

	now := time.Now().UnixMilli()

	// Convert elems to bytes
	elemsBytes := make([][]byte, len(elems))
	for i, elem := range elems {
		b, err := core.ToBytes(elem)
		if err != nil {
			return 0, err
		}
		elemsBytes[i] = b
	}

	var count int64
	err := tx.tx.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", tx.dbIdx, key).
		Where("rkey.ktype = 5").
		Scopes(store.NotExpired(now)).
		Where("rzset.elem IN ?", elemsBytes).
		Count(&count).Error
	return int(count), err
}

func (tx *Tx) add(key string, elem any, score float64) error {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return err
	}
	now := time.Now().UnixMilli()

	return tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create the key with type checking
		var keyMeta store.RKey
		err := txInner.Where("kname = ?", key).First(&keyMeta).Error
		switch err {
		case nil:
			// Key exists, check type
			if keyMeta.KType != 5 {
				return core.ErrKeyType
			}
			// Update version and mtime
			txInner.Model(&keyMeta).Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
			})
		case gorm.ErrRecordNotFound:
			// Create new key
			keyMeta = store.RKey{
				KName:      key,
				KType:      5,
				KVer:       1,
				ModifiedAt: now,
				KLen:       0,
			}
			if err := txInner.Create(&keyMeta).Error; err != nil {
				return err
			}
		default:
			return err
		}

		// Get the key ID (for newly created key)
		if keyMeta.ID == 0 {
			err = txInner.Model(&store.RKey{}).
				Where("kname = ?", key).
				First(&keyMeta).Error
			if err != nil {
				return err
			}
		}

		// Check if element exists
		var existing store.RZSet
		err = txInner.Where("kid = ? AND elem = ?", keyMeta.ID, elemb).First(&existing).Error
		isNew := (err == gorm.ErrRecordNotFound)

		// Insert or update the element
		rzset := store.RZSet{
			KID:   keyMeta.ID,
			Elem:  elemb,
			Score: score,
		}
		err = txInner.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "kid"}, {Name: "elem"}},
			DoUpdates: clause.AssignmentColumns([]string{"score"}),
		}).Create(&rzset).Error
		if err != nil {
			return err
		}

		// Update len if new element
		if isNew {
			return txInner.Model(&store.RKey{}).
				Where("id = ?", keyMeta.ID).
				UpdateColumn("klen", gorm.Expr("klen + 1")).Error
		}

		return nil
	})
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
	tx      *Tx
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

	// Get the key ID
	var keyMeta store.RKey
	err := c.tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", c.tx.dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
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
	err = c.tx.tx.Model(&store.RZSet{}).
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

	// Get the key ID
	var keyMeta store.RKey
	err := c.tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", c.tx.dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
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

	query := c.tx.tx.Model(&store.RZSet{}).
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
	tx      *Tx
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
	if c.db != nil {
		var n int
		err := c.db.update(func(tx *Tx) error {
			var err error
			n, err = c.run(tx)
			return err
		})
		return n, err
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return 0, nil
}

func (c DeleteCmd) run(tx *Tx) (n int, err error) {
	now := time.Now().UnixMilli()

	if c.byRank != nil {
		n, err = c.deleteRank(tx, now)
	} else if c.byScore != nil {
		n, err = c.deleteScore(tx, now)
	} else {
		return 0, nil
	}
	if err != nil || n == 0 {
		return 0, err
	}

	err = c.updateKey(tx, now, n)
	return n, err
}

// deleteRank removes elements from a set by rank.
// Returns the number of elements removed.
func (c DeleteCmd) deleteRank(tx *Tx, now int64) (int, error) {
	// Check start and stop values.
	if c.byRank.start < 0 || c.byRank.stop < 0 {
		return 0, nil
	}

	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Get elements to delete by rank using GORM Offset/Limit
	var elemsToDelete []struct{ Elem []byte }
	err = tx.tx.Model(&store.RZSet{}).
		Select("elem").
		Where("kid = ?", keyMeta.ID).
		Order("score ASC, elem ASC").
		Offset(c.byRank.start).
		Limit(c.byRank.stop - c.byRank.start + 1).
		Find(&elemsToDelete).Error
	if err != nil {
		return 0, err
	}

	if len(elemsToDelete) == 0 {
		return 0, nil
	}

	// Collect element values for batch delete
	elemVals := make([][]byte, len(elemsToDelete))
	for i, e := range elemsToDelete {
		elemVals[i] = e.Elem
	}

	// Delete elements
	result := tx.tx.Where("kid = ? AND elem IN ?", keyMeta.ID, elemVals).Delete(&store.RZSet{})
	if result.Error != nil {
		return 0, result.Error
	}

	return int(result.RowsAffected), nil
}

// deleteScore removes elements from a set by score.
// Returns the number of elements removed.
func (c DeleteCmd) deleteScore(tx *Tx, now int64) (int, error) {
	// Get the key ID
	var keyMeta store.RKey
	err := tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, c.key).
		Scopes(store.NotExpired(now)).
		First(&keyMeta).Error
	if err == gorm.ErrRecordNotFound {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	// Delete elements by score range
	result := tx.tx.Where("kid = ? AND score BETWEEN ? AND ?", keyMeta.ID, c.byScore.start, c.byScore.stop).Delete(&store.RZSet{})
	if result.Error != nil {
		return 0, result.Error
	}
	return int(result.RowsAffected), nil
}

// updateKey updates the key after deleting the elements.
func (c DeleteCmd) updateKey(tx *Tx, now int64, n int) error {
	if n == 0 {
		return nil
	}
	return tx.tx.Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = 5", tx.dbIdx, c.key).
		Updates(map[string]any{
			"kver":        gorm.Expr("kver + 1"),
			"modified_at": now,
			"klen":        gorm.Expr("klen - ?", n),
		}).Error
}

// InterCmd intersects multiple sets.
type InterCmd struct {
	db        *DB
	tx        *Tx
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
func (c InterCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.store.Dialect, c.db.store.DB, c.db.dbIdx)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store intersects multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// If any of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (c InterCmd) Store() (int, error) {
	if c.db != nil {
		var count int
		err := c.db.update(func(tx *Tx) error {
			var err error
			count, err = c.store(tx)
			return err
		})
		return count, err
	}
	if c.tx != nil {
		return c.store(c.tx)
	}
	return 0, nil
}

// run returns the intersection of multiple sets.
func (c InterCmd) run(tx *Tx) ([]SetItem, error) {
	now := time.Now().UnixMilli()

	// Get key IDs for all keys
	var keyIDs []int
	err := tx.tx.Model(&store.RKey{}).
		Where("kname IN ? AND ktype = 5", c.keys).
		Scopes(store.NotExpired(now)).
		Pluck("id", &keyIDs).Error
	if err != nil {
		return nil, err
	}

	if len(keyIDs) != len(c.keys) {
		// Some keys don't exist
		return nil, nil
	}

	// Build aggregation expression
	aggExpr := "SUM(score) as score"
	switch c.aggregate {
	case AggregateMin:
		aggExpr = "MIN(score) as score"
	case AggregateMax:
		aggExpr = "MAX(score) as score"
	}

	// Query intersection using GROUP BY and HAVING
	var items []SetItem
	err = tx.tx.Model(&store.RZSet{}).
		Select("elem, "+aggExpr).
		Where("kid IN ?", keyIDs).
		Group("elem").
		Having("COUNT(DISTINCT kid) = ?", len(c.keys)).
		Scan(&items).Error

	return items, err
}

// store intersects multiple sets and stores the result in a new set.
func (c InterCmd) store(tx *Tx) (int, error) {
	now := time.Now().UnixMilli()

	// Get the intersection items first
	items, err := c.run(tx)
	if err != nil {
		return 0, err
	}

	var resultCount int
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create the destination key with type checking
		var destKey store.RKey
		err := txInner.Where("kname = ?", c.dest).First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != 5 {
				return core.ErrKeyType
			}
			// Delete existing elements
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RZSet{})
			// Reset len to 0 (will be updated later)
			destKey.KLen = 0
		case gorm.ErrRecordNotFound:
			// Create new key
			destKey = store.RKey{
				KName:      c.dest,
				KType:      5,
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

		// Get the destination key ID (for newly created key)
		if destKey.ID == 0 {
			err = txInner.Model(&store.RKey{}).
				Where("kname = ?", c.dest).
				First(&destKey).Error
			if err != nil {
				return err
			}
		}

		// If no items to insert, delete the destination key
		if len(items) == 0 {
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RZSet{})
			txInner.Delete(&destKey)
			resultCount = 0
			return nil
		}

		// Insert items into destination set
		for _, item := range items {
			rzset := store.RZSet{
				KID:   destKey.ID,
				Elem:  []byte(item.Elem),
				Score: item.Score,
			}
			err = txInner.Create(&rzset).Error
			if err != nil {
				return err
			}
		}

		// Update destination key metadata
		resultCount = len(items)
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]interface{}{
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
	tx        *Tx
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
func (c UnionCmd) Run() ([]SetItem, error) {
	if c.db != nil {
		tx := NewTx(c.db.store.Dialect, c.db.store.DB, c.db.dbIdx)
		return c.run(tx)
	}
	if c.tx != nil {
		return c.run(c.tx)
	}
	return nil, nil
}

// Store unions multiple sets and stores the result in a new set.
// Returns the number of elements in the resulting set.
// If the destination key already exists, it is fully overwritten
// (all old elements are removed and the new ones are inserted).
// If the destination key already exists and is not a set, returns ErrKeyType.
// Ignores the source keys that do not exist or are not sets.
// If all of the source keys do not exist or are not sets, does nothing,
// except deleting the destination key if it exists.
func (c UnionCmd) Store() (int, error) {
	if c.db != nil {
		var count int
		err := c.db.update(func(tx *Tx) error {
			var err error
			count, err = c.store(tx)
			return err
		})
		return count, err
	}
	if c.tx != nil {
		return c.store(c.tx)
	}
	return 0, nil
}

// run returns the union of multiple sets.
func (c UnionCmd) run(tx *Tx) ([]SetItem, error) {
	now := time.Now().UnixMilli()

	// Get key IDs for all keys
	var keyIDs []int
	err := tx.tx.Model(&store.RKey{}).
		Where("kname IN ? AND ktype = 5", c.keys).
		Scopes(store.NotExpired(now)).
		Pluck("id", &keyIDs).Error
	if err != nil {
		return nil, err
	}

	if len(keyIDs) == 0 {
		return nil, nil
	}

	// Build aggregation expression
	aggExpr := "SUM(score) as score"
	switch c.aggregate {
	case AggregateMin:
		aggExpr = "MIN(score) as score"
	case AggregateMax:
		aggExpr = "MAX(score) as score"
	}

	// Query union using GROUP BY
	var items []SetItem
	err = tx.tx.Model(&store.RZSet{}).
		Select("elem, "+aggExpr).
		Where("kid IN ?", keyIDs).
		Group("elem").
		Scan(&items).Error

	return items, err
}

// store unions multiple sets and stores the result in a new set.
func (c UnionCmd) store(tx *Tx) (int, error) {
	now := time.Now().UnixMilli()

	// Get the union items first
	items, err := c.run(tx)
	if err != nil {
		return 0, err
	}

	var resultCount int
	err = tx.tx.Transaction(func(txInner *gorm.DB) error {
		// Get or create the destination key with type checking
		var destKey store.RKey
		err := txInner.Where("kname = ?", c.dest).First(&destKey).Error
		switch err {
		case nil:
			// Key exists, check type
			if destKey.KType != 5 {
				return core.ErrKeyType
			}
			// Delete existing elements
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RZSet{})
		case gorm.ErrRecordNotFound:
			// Create new key
			destKey = store.RKey{
				KName:      c.dest,
				KType:      5,
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

		// Get the destination key ID (for newly created key)
		if destKey.ID == 0 {
			err = txInner.Model(&store.RKey{}).
				Where("kname = ?", c.dest).
				First(&destKey).Error
			if err != nil {
				return err
			}
		}

		if len(items) == 0 {
			// Delete the destination key if it exists and no items to insert
			txInner.Where("kid = ?", destKey.ID).Delete(&store.RZSet{})
			txInner.Delete(&destKey)
			resultCount = 0
			return nil
		}

		// Insert items into destination set
		for _, item := range items {
			rzset := store.RZSet{
				KID:   destKey.ID,
				Elem:  []byte(item.Elem),
				Score: item.Score,
			}
			err = txInner.Create(&rzset).Error
			if err != nil {
				return err
			}
		}

		// Update destination key metadata
		resultCount = len(items)
		return txInner.Model(&store.RKey{}).
			Where("id = ?", destKey.ID).
			Updates(map[string]interface{}{
				"kver":        gorm.Expr("kver + 1"),
				"modified_at": now,
				"klen":        len(items),
			}).Error
	})

	return resultCount, err
}
