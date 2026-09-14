// Package rzset is a database-backed sorted set repository.
// It provides methods to interact with sorted sets in the database.
package rzset

import (
	"context"
	"errors"
	"strconv"
	"strings"
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
	store *store.Store
	dbIdx int
	ctx   context.Context
}

// New connects to the sorted set repository.
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

// Add adds or updates an element in a set.
// Returns true if the element was created, false if it was updated.
// If the key does not exist, creates it.
// If the key exists but is not a set, returns ErrKeyType.
func (d *DB) Add(key string, elem any, score float64) (bool, error) {
	elemb, err := core.ToBytes(elem)
	if err != nil {
		return false, err
	}
	created := false
	item := zsetWriteItem{elem: elemb, score: score, created: &created}
	err = d.store.TransactionWriteItems(d.ctx,
		d.writeScope(key)+"\x00"+string(elemb), item,
		func(tx *gorm.DB, _ store.Dialect, items []any) error {
			last := items[len(items)-1].(zsetWriteItem)
			current, getErr := d.getScoreInDB(tx, key, last.elem)
			existed := getErr == nil
			if getErr != nil && !errors.Is(getErr, core.ErrNotFound) {
				return getErr
			}
			if existed {
				if current == last.score {
					return nil
				}
				_, updateErr := d.updateExisting(tx, key, last.elem, last.score)
				return updateErr
			}

			rowCreated, addErr := d.addNonBatched(tx, key, last.elem, last.score)
			if addErr != nil {
				return addErr
			}
			if rowCreated && len(items) > 0 {
				*items[0].(zsetWriteItem).created = true
			}
			return nil
		})
	return created, err
}

type zsetWriteItem struct {
	elem    []byte
	score   float64
	created *bool
}

func (d *DB) getScoreInDB(db *gorm.DB, key string, elem []byte) (float64, error) {
	var result struct {
		Score float64
	}
	err := db.Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id AND rkey.ktype = ?", core.TypeZSet.Value()).
		Where("rkey.kdb = ? AND rkey.kname = ? AND rzset.elem = ?", d.dbIdx, key, elem).
		Scopes(store.NotExpired(time.Now().UnixMilli())).
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

func (d *DB) addNonBatched(db *gorm.DB, key string, elemb []byte, score float64) (bool, error) {
	var created bool
	err := db.Transaction(func(tx *gorm.DB) error {
		dialect := d.store.Dialect
		now := time.Now().UnixMilli()
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key,
			core.TypeZSet.Value(), now, 0)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeZSet.Value() {
			return core.ErrKeyType
		}

		row := store.RZSet{KID: rkey.ID, Elem: elemb, Score: score}
		result := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "elem"}}, &row)
		if result.Error != nil {
			return result.Error
		}
		created = result.RowsAffected == 1
		if created {
			return nil
		}
		return tx.Model(&store.RZSet{}).
			Where("kid = ? AND elem = ?", rkey.ID, elemb).
			Update("score", score).Error
	})
	return created, err
}

func (d *DB) writeScope(key string) string {
	return "zset:write:" + strconv.Itoa(d.dbIdx) + "\x00" + key
}

// updateExisting performs a one-statement score update of a hot member.
func (d *DB) updateExisting(db *gorm.DB, key string, elem []byte, score float64) (bool, error) {
	if d.store.Dialect == store.DialectMySQL {
		result := db.Exec(`
			UPDATE rkey AS k
			JOIN rzset AS z ON z.kid = k.id
			SET z.score = ?
			WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ? AND z.elem = ?`,
			score, d.dbIdx, key, core.TypeZSet.Value(), elem)
		if result.Error != nil {
			return false, result.Error
		}
		return result.RowsAffected > 0, nil
	}

	var result *gorm.DB
	switch d.store.Dialect {
	case store.DialectSQLite, store.DialectPostgres:
		result = db.Exec(`
			UPDATE rzset
			SET score = ?
			WHERE kid = (
				SELECT id FROM rkey
				WHERE kdb = ? AND kname = ? AND ktype = ?
			) AND elem = ?`,
			score, d.dbIdx, key, core.TypeZSet.Value(), elem)
	default:
		return false, nil
	}
	if result.Error != nil {
		return false, result.Error
	}
	return result.RowsAffected > 0, nil
}

// AddMany adds or updates multiple elements in a set.
func (d *DB) AddMany(key string, items map[any]float64) (int, error) {
	if len(items) == 0 {
		return 0, nil
	}

	elems := make(map[string]float64, len(items))
	for elem, score := range items {
		elemb, err := core.ToBytes(elem)
		if err != nil {
			return 0, err
		}
		elems[string(elemb)] = score
	}

	if len(elems) == 1 {
		for elem, score := range elems {
			created, err := d.Add(key, elem, score)
			if err != nil {
				return 0, err
			}
			if created {
				return 1, nil
			}
			return 0, nil
		}
	}

	var created int
	err := d.store.TransactionRetry(d.ctx, func(tx *gorm.DB, dialect store.Dialect) error {
		now := time.Now().UnixMilli()
		rkey, err := store.EnsureKey(tx, dialect, d.dbIdx, key, core.TypeZSet.Value(), now, 0)
		if err != nil {
			return err
		}
		if rkey.KType != core.TypeZSet.Value() {
			return core.ErrKeyType
		}

		rows := make([]store.RZSet, 0, len(elems))
		elemBytes := make([][]byte, 0, len(elems))
		caseArgs := make([]interface{}, 0, len(elems)*2)
		for elemb, score := range elems {
			elem := []byte(elemb)
			rows = append(rows, store.RZSet{KID: rkey.ID, Elem: elem, Score: score})
			elemBytes = append(elemBytes, elem)
			caseArgs = append(caseArgs, elem, score)
		}

		result := store.CreateIgnoreConflict(tx, dialect,
			[]clause.Column{{Name: "kid"}, {Name: "elem"}}, &rows)
		if result.Error != nil {
			return result.Error
		}
		created = int(result.RowsAffected)
		if created == len(elems) {
			return nil
		}
		return tx.Model(&store.RZSet{}).
			Where("kid = ? AND elem IN ?", rkey.ID, elemBytes).
			Update("score", gorm.Expr(
				"CASE elem "+strings.Repeat("WHEN ? THEN ? ", len(elems))+"ELSE score END",
				caseArgs...,
			)).Error
	})

	return created, err
}

// Count returns the number of elements in a set with a score between min and max.
func (d *DB) Count(key string, min, max float64) (int, error) {
	now := time.Now().UnixMilli()
	var count int64
	err := d.db().Model(&store.RZSet{}).
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Where("rkey.ktype = ?", core.TypeZSet.Value()).
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

	err = d.store.Transaction(d.ctx, func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var rkey store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeZSet.Value()).
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
	err = d.db().Model(&store.RZSet{}).
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
	err = d.db().Model(&store.RZSet{}).
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
	err = d.db().Model(&store.RZSet{}).
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
	err = d.db().Model(&store.RZSet{}).
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
	err = d.db().Model(&store.RZSet{}).
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

	newScore := 0.0
	item := zsetIncrWriteItem{delta: delta, score: &newScore}
	err = d.store.TransactionWriteItems(d.ctx,
		d.writeScope(key)+"\x00"+string(elemb),
		item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			now := time.Now().UnixMilli()
			incs := make([]zsetIncrWriteItem, 0, len(items))
			total := 0.0
			for _, raw := range items {
				inc := raw.(zsetIncrWriteItem)
				incs = append(incs, inc)
				total += inc.delta
			}
			if dialect == store.DialectPostgres {
				var result struct {
					Score *float64 `gorm:"column:score"`
				}
				err := tx.Raw(`
				UPDATE rzset AS z
				SET score = z.score + ?
				FROM rkey AS k
				WHERE z.kid = k.id
				  AND k.kdb = ? AND k.kname = ? AND k.ktype = ? AND z.elem = ?
				RETURNING z.score`,
					total, d.dbIdx, key, core.TypeZSet.Value(), elemb).Scan(&result).Error
				if err == nil && result.Score != nil {
					assignZSetIncrResults(incs, *result.Score-total)
					if len(incs) > 0 {
						*incs[len(incs)-1].score = *result.Score
					}
					return nil
				}
				if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
					return err
				}
			}
			if dialect == store.DialectMySQL {
				result := tx.Exec(`
				UPDATE rzset AS z
				JOIN rkey AS k ON z.kid = k.id
				SET z.score = z.score + ?
				WHERE k.kdb = ? AND k.kname = ? AND k.ktype = ? AND z.elem = ?`,
					total, d.dbIdx, key, core.TypeZSet.Value(), elemb)
				if result.Error != nil {
					return result.Error
				}
				if result.RowsAffected > 0 {
					var final float64
					if err := tx.Model(&store.RKey{}).
						Select("rzset.score").
						Joins("JOIN rzset ON rzset.kid = rkey.id AND rzset.elem = ?", elemb).
						Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
						First(&final).Error; err != nil {
						return err
					}
					assignZSetIncrResults(incs, final-total)
					if len(incs) > 0 {
						*incs[len(incs)-1].score = final
					}
					return nil
				}
			}

			var current struct {
				ID    int     `gorm:"column:id"`
				KType int     `gorm:"column:ktype"`
				Score float64 `gorm:"column:score"`
			}
			currentQuery := tx.Model(&store.RKey{}).
				Select("rkey.id, rkey.ktype, rzset.score").
				Joins("JOIN rzset ON rzset.kid = rkey.id AND rzset.elem = ?", elemb).
				Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key)
			if dialect == store.DialectMySQL || dialect == store.DialectPostgres {
				currentQuery = currentQuery.Clauses(clause.Locking{Strength: "UPDATE"})
			}
			if err := currentQuery.First(&current).Error; err == nil {
				if current.KType != core.TypeZSet.Value() {
					return core.ErrKeyType
				}
				final := assignZSetIncrResults(incs, current.Score)
				return tx.Model(&store.RZSet{}).
					Where("kid = ? AND elem = ?", current.ID, elemb).
					Update("score", final).Error
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
					KType:      core.TypeZSet.Value(),
					KVer:       1,
					ModifiedAt: now,
					KLen:       1,
				}
				if err := tx.Create(&rkey).Error; err != nil {
					return err
				}
				final := assignZSetIncrResults(incs, 0)
				rzset := store.RZSet{
					KID:   rkey.ID,
					Elem:  elemb,
					Score: final,
				}
				if err := tx.Create(&rzset).Error; err != nil {
					return err
				}
				return nil

			case err != nil:
				return err

			case rkey.KType != core.TypeZSet.Value():
				return core.ErrKeyType

			default:
				var existing store.RZSet
				err = tx.Where("kid = ? AND elem = ?", rkey.ID, elemb).First(&existing).Error

				if errors.Is(err, gorm.ErrRecordNotFound) {
					final := assignZSetIncrResults(incs, 0)
					rzset := store.RZSet{
						KID:   rkey.ID,
						Elem:  elemb,
						Score: final,
					}
					if err := tx.Create(&rzset).Error; err != nil {
						return err
					}
					if err := tx.Model(&store.RKey{}).
						Where("id = ?", rkey.ID).
						Update("klen", gorm.Expr("klen + 1")).Error; err != nil {
						return err
					}
				} else if err != nil {
					return err
				} else {
					existing.Score = assignZSetIncrResults(incs, existing.Score)
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

func assignZSetIncrResults(items []zsetIncrWriteItem, start float64) float64 {
	current := start
	for _, item := range items {
		current += item.delta
		*item.score = current
	}
	return current
}

type zsetIncrWriteItem struct {
	delta float64
	score *float64
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
	err := d.db().Model(&store.RZSet{}).
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
	err := d.db().Model(&store.RKey{}).
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
	err = d.db().Where("kid = ?", rkey.ID).
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
	err := d.db().Model(&store.RKey{}).
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
	query := d.db().Where("kid = ?", rkey.ID).
		Order("score ASC, elem ASC, id ASC").
		Limit(count)

	// Decode cursor and get the last item's score from DB
	if cursor > 0 {
		// Query the score of the cursor ID
		var lastItem store.RZSet
		err := d.db().Select("score").
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

	err := d.store.Transaction(d.ctx, func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeZSet.Value()).
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

	err := d.store.Transaction(d.ctx, func(tx *gorm.DB, _ store.Dialect) error {
		now := time.Now().UnixMilli()
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeZSet.Value()).
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
	err := d.db().Model(&store.RKey{}).
		Where("kdb = ? AND kname = ? AND ktype = ?", d.dbIdx, key, core.TypeZSet.Value()).
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
	err = d.db().Model(&store.RZSet{}).
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
	err := d.db().Model(&store.RZSet{}).
		Select("rzset.elem as elem, rzset.score as score").
		Joins("JOIN rkey ON rzset.kid = rkey.id").
		Where("rkey.kdb = ? AND rkey.kname = ?", d.dbIdx, key).
		Where("rkey.ktype = ?", core.TypeZSet.Value()).
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
		Where("kdb = ? AND kname = ? AND ktype = ?", dbIdx, c.key, core.TypeZSet.Value()).
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
		Where("kdb = ? AND kname = ? AND ktype = ?", dbIdx, c.key, core.TypeZSet.Value()).
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
	err := c.db.store.Transaction(c.db.ctx, func(tx *gorm.DB, _ store.Dialect) error {
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", c.db.dbIdx, c.key, core.TypeZSet.Value()).
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
	err := c.db.store.Transaction(c.db.ctx, func(tx *gorm.DB, _ store.Dialect) error {
		var keyMeta store.RKey
		err := tx.Model(&store.RKey{}).
			Where("kdb = ? AND kname = ? AND ktype = ?", c.db.dbIdx, c.key, core.TypeZSet.Value()).
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
		Where("kdb = ? AND kname IN ? AND ktype = ?", c.db.dbIdx, c.keys, core.TypeZSet.Value()).
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
	resultCount := 0
	item := zsetStoreWriteItem{
		keys:      c.keys,
		aggregate: c.aggregate,
		count:     &resultCount,
	}
	err := c.db.store.TransactionWriteItems(c.db.ctx,
		"zset:store:inter:"+strconv.Itoa(c.db.dbIdx)+"\x00"+c.dest, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			return c.db.writeZSetStoreItems(tx, dialect, c.dest, items,
				func(keys []string, aggregate string) ([]SetItem, error) {
					run := InterCmd{db: c.db, keys: keys, aggregate: aggregate}
					return run.runTx(tx)
				})
		})
	return resultCount, err
}

func (d *DB) writeZSetStoreItems(tx *gorm.DB, _ store.Dialect, dest string,
	items []any, compute func(keys []string, aggregate string) ([]SetItem, error)) error {
	groups := make(map[string][]zsetStoreWriteItem)
	order := make([]string, 0)
	for _, raw := range items {
		item := raw.(zsetStoreWriteItem)
		signature := item.aggregate + "\x00" + zsetStoreKeysSignature(item.keys)
		if _, exists := groups[signature]; !exists {
			order = append(order, signature)
		}
		groups[signature] = append(groups[signature], item)
	}

	for _, signature := range order {
		group := groups[signature]
		last := group[len(group)-1]
		items, err := compute(last.keys, last.aggregate)
		if err != nil {
			return err
		}
		count, err := d.storeResultItems(tx, dest, items)
		if err != nil {
			return err
		}
		for _, item := range group {
			*item.count = count
		}
	}
	return nil
}

func zsetStoreKeysSignature(keys []string) string {
	var signature strings.Builder
	for _, key := range keys {
		signature.WriteString(strconv.Itoa(len(key)))
		signature.WriteByte(':')
		signature.WriteString(key)
		signature.WriteByte('\x1f')
	}
	return signature.String()
}

type zsetStoreWriteItem struct {
	keys      []string
	aggregate string
	count     *int
}

func (d *DB) storeResult(tx *gorm.DB, dialect store.Dialect, dest string, compute func() ([]SetItem, error)) (int, error) {
	items, err := compute()
	if err != nil {
		return 0, err
	}
	return d.storeResultItems(tx, dest, items)
}

func (d *DB) storeResultItems(tx *gorm.DB, dest string, items []SetItem) (int, error) {
	now := time.Now().UnixMilli()
	destKey, err := store.LockKey(tx, d.store.Dialect, d.dbIdx, dest)
	if errors.Is(err, gorm.ErrRecordNotFound) {
		destKey, err = store.EnsureKeyLocked(tx, d.store.Dialect, d.dbIdx, dest,
			core.TypeZSet.Value(), now, 0)
	}
	if err != nil {
		return 0, err
	}
	if destKey.KType != core.TypeZSet.Value() {
		return 0, core.ErrKeyType
	}
	expired := destKey.ExpireAt != nil && *destKey.ExpireAt <= now

	var existing []store.RZSet
	if err := tx.Where("kid = ?", destKey.ID).Find(&existing).Error; err != nil {
		return 0, err
	}
	if !expired {
		existingItems := make(map[string]float64, len(existing))
		for _, row := range existing {
			existingItems[string(row.Elem)] = row.Score
		}
		resultItems := make(map[string]float64, len(items))
		for _, item := range items {
			resultItems[string(item.Elem)] = item.Score
		}
		if len(existingItems) == len(resultItems) {
			unchanged := true
			for elem, score := range resultItems {
				if existingScore, ok := existingItems[elem]; !ok || existingScore != score {
					unchanged = false
					break
				}
			}
			if unchanged {
				return len(items), nil
			}
		}
	}

	if err := tx.Where("kid = ?", destKey.ID).Delete(&store.RZSet{}).Error; err != nil {
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
			Where("id = ?", destKey.ID).
			Updates(updates).Error
	}

	rows := make([]store.RZSet, len(items))
	for i, item := range items {
		rows[i] = store.RZSet{KID: destKey.ID, Elem: []byte(item.Elem), Score: item.Score}
	}
	if err := tx.Create(&rows).Error; err != nil {
		return 0, err
	}
	return len(items), tx.Model(&store.RKey{}).
		Where("id = ?", destKey.ID).
		Updates(updates).Error
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
		Where("kdb = ? AND kname IN ? AND ktype = ?", c.db.dbIdx, c.keys, core.TypeZSet.Value()).
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
	resultCount := 0
	item := zsetStoreWriteItem{
		keys:      c.keys,
		aggregate: c.aggregate,
		count:     &resultCount,
	}
	err := c.db.store.TransactionWriteItems(c.db.ctx,
		"zset:store:union:"+strconv.Itoa(c.db.dbIdx)+"\x00"+c.dest, item,
		func(tx *gorm.DB, dialect store.Dialect, items []any) error {
			return c.db.writeZSetStoreItems(tx, dialect, c.dest, items,
				func(keys []string, aggregate string) ([]SetItem, error) {
					run := UnionCmd{db: c.db, keys: keys, aggregate: aggregate}
					return run.runTx(tx)
				})
		})
	return resultCount, err
}
