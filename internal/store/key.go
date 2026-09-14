package store

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EnsureKey atomically creates a missing key record and returns the current
// record. It is safe to call concurrently for the same logical key.
func EnsureKey(db *gorm.DB, dialect Dialect, dbIdx int, name string, keyType int, now int64, initialLen int) (RKey, error) {
	current, err := findKey(db, dbIdx, name)
	if err == nil {
		return current, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return RKey{}, err
	}
	key, _, err := insertKeyIfAbsent(db, dialect, dbIdx, name, keyType, now, initialLen)
	return key, err
}

// EnsureKeyLocked additionally locks the key row. Operations that derive new
// values from existing rows, such as list positions, need this variant.
func EnsureKeyLocked(db *gorm.DB, dialect Dialect, dbIdx int, name string, keyType int, now int64, initialLen int) (RKey, error) {
	_, err := findKey(db, dbIdx, name)
	if err == nil {
		return LockKey(db, dialect, dbIdx, name)
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return RKey{}, err
	}
	key, created, err := insertKeyIfAbsent(db, dialect, dbIdx, name, keyType, now, initialLen)
	if err != nil || created {
		return key, err
	}
	return LockKey(db, dialect, dbIdx, name)
}

// InsertKeyIfAbsent creates a key and reports whether this call created it.
// When another transaction wins the insert, the existing key is returned.
func InsertKeyIfAbsent(db *gorm.DB, dialect Dialect, dbIdx int, name string, keyType int, now int64, initialLen int) (RKey, bool, error) {
	current, err := findKey(db, dbIdx, name)
	if err == nil {
		return current, false, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return RKey{}, false, err
	}
	return insertKeyIfAbsent(db, dialect, dbIdx, name, keyType, now, initialLen)
}

// CreateIgnoreConflict inserts rows without replacing a conflicting row. MySQL
// uses INSERT IGNORE because GORM's DO NOTHING translation reports a matched
// duplicate as changed when CLIENT_FOUND_ROWS is enabled.
func CreateIgnoreConflict(db *gorm.DB, dialect Dialect, columns []clause.Column, value any) *gorm.DB {
	if dialect == DialectMySQL {
		return db.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(value)
	}
	return db.Clauses(clause.OnConflict{Columns: columns, DoNothing: true}).Create(value)
}

func insertKeyIfAbsent(db *gorm.DB, dialect Dialect, dbIdx int, name string, keyType int, now int64, initialLen int) (RKey, bool, error) {
	candidate := RKey{
		KDB:        dbIdx,
		KName:      name,
		KType:      keyType,
		KVer:       1,
		ModifiedAt: now,
		KLen:       initialLen,
	}

	result := CreateIgnoreConflict(db, dialect,
		[]clause.Column{{Name: "kdb"}, {Name: "kname"}}, &candidate)
	if result.Error != nil {
		return RKey{}, false, result.Error
	}
	if result.RowsAffected == 1 {
		return candidate, true, nil
	}

	if dialect == DialectMySQL || dialect == DialectPostgres {
		current, err := lockKey(db, dialect, dbIdx, name)
		return current, false, err
	}
	current, err := findKey(db, dbIdx, name)
	return current, false, err
}

// LockKey locks and returns an existing key row. It does not create a key.
func LockKey(db *gorm.DB, dialect Dialect, dbIdx int, name string) (RKey, error) {
	return lockKey(db, dialect, dbIdx, name)
}

func lockKey(db *gorm.DB, dialect Dialect, dbIdx int, name string) (RKey, error) {
	query := db.Model(&RKey{}).
		Where("kdb = ? AND kname = ?", dbIdx, name)
	if dialect == DialectMySQL || dialect == DialectPostgres {
		query = query.Clauses(clause.Locking{Strength: "UPDATE"})
	}

	var current RKey
	err := query.First(&current).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return RKey{}, err
	}
	if err != nil {
		return RKey{}, err
	}
	return current, nil
}

func findKey(db *gorm.DB, dbIdx int, name string) (RKey, error) {
	var current RKey
	err := db.Model(&RKey{}).
		Where("kdb = ? AND kname = ?", dbIdx, name).
		First(&current).Error
	return current, err
}
