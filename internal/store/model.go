package store

// RKey is the key metadata table.
// Types:
//   - 1: string
//   - 2: list
//   - 3: set
//   - 4: hash
//   - 5: zset (sorted set)
type RKey struct {
	ID         int    `gorm:"column:id;primaryKey;autoIncrement"`
	KName      string `gorm:"column:kname;size:255;uniqueIndex:rkey_db_key_idx,priority:2;not null"`
	KDB        int    `gorm:"column:kdb;uniqueIndex:rkey_db_key_idx,priority:1;not null;default:0"` // logical database 0-15
	KType      int    `gorm:"column:ktype;not null"`
	KVer       int    `gorm:"column:kver;not null;default:0"`
	ExpireAt   *int64 `gorm:"column:expire_at;index:rkey_expire_at_idx"`
	ModifiedAt int64  `gorm:"column:modified_at;not null;default:0"`
	KLen       int    `gorm:"column:klen;not null;default:0"` // Length of the key value
}

// TableName returns the table name for GORM.
func (RKey) TableName() string { return "rkey" }

// RString stores string values.
// One-to-one relationship with RKey.
type RString struct {
	KID  int    `gorm:"column:kid;primaryKey"`
	KVal []byte `gorm:"column:kval;not null"`
}

// TableName returns the table name for GORM.
func (RString) TableName() string { return "rstring" }

// RHash stores hash field-value pairs.
// Composite primary key on (kid, field).
type RHash struct {
	ID     int    `gorm:"column:id;primaryKey;autoIncrement"` // For Postgres/MySQL
	KID    int    `gorm:"column:kid;uniqueIndex:rhash_uniq_idx,priority:1;not null"`
	KField string `gorm:"column:kfield;size:255;uniqueIndex:rhash_uniq_idx,priority:2;not null"`
	KVal   []byte `gorm:"column:kval;not null"`
}

// TableName returns the table name for GORM.
func (RHash) TableName() string { return "rhash" }

// RList stores list elements with position.
// Ordered by position within each key.
// Position is an int64 to avoid float64 precision issues with large lists.
type RList struct {
	ID   int    `gorm:"column:id;primaryKey;autoIncrement"` // For Postgres/MySQL
	KID  int    `gorm:"column:kid;uniqueIndex:rlist_uniq_idx,priority:1;not null"`
	Pos  int64  `gorm:"column:pos;uniqueIndex:rlist_uniq_idx,priority:2;not null"`
	Elem []byte `gorm:"column:elem;not null"`
}

// TableName returns the table name for GORM.
func (RList) TableName() string { return "rlist" }

// RSet stores set elements.
// Composite unique index on (kid, elem).
type RSet struct {
	ID   int    `gorm:"column:id;primaryKey;autoIncrement"` // For Postgres/MySQL
	KID  int    `gorm:"column:kid;uniqueIndex:rset_uniq_idx,priority:1;not null"`
	Elem []byte `gorm:"column:elem;uniqueIndex:rset_uniq_idx,priority:2,length:255;not null"`
}

// TableName returns the table name for GORM.
func (RSet) TableName() string { return "rset" }

// RZSet stores sorted set elements with score.
// Composite unique index on (kid, elem) and additional index on (kid, score, elem).
type RZSet struct {
	ID    int     `gorm:"column:id;primaryKey;autoIncrement"` // For Postgres/MySQL
	KID   int     `gorm:"column:kid;uniqueIndex:rzset_uniq_idx,priority:1;not null"`
	Elem  []byte  `gorm:"column:elem;uniqueIndex:rzset_uniq_idx,priority:2,length:255;not null"`
	Score float64 `gorm:"column:score;index:rzset_score_idx,priority:2;not null;default:0"`
}

// TableName returns the table name for GORM.
func (RZSet) TableName() string { return "rzset" }
