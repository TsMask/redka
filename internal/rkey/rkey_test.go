package rkey

import (
	"testing"

	"github.com/tsmask/redka/internal/store"
)

func TestDeleteAllInDB(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	seedLogicalDB(t, db, 0)
	seedLogicalDB(t, db, 1)

	if err := New(db).WithDB(0).DeleteAllInDB(); err != nil {
		t.Fatalf("DeleteAllInDB: %v", err)
	}

	if count := repositoryLen(t, New(db).WithDB(0)); count != 0 {
		t.Fatalf("db 0 length after FLUSHDB = %d, want 0", count)
	}
	if count := repositoryLen(t, New(db).WithDB(1)); count != 5 {
		t.Fatalf("db 1 length after DB 0 FLUSHDB = %d, want 5", count)
	}
	valueTables := []string{"rstring", "rlist", "rset", "rhash", "rzset"}
	for _, table := range valueTables {
		if count := joinedValueCount(t, db, 0, table); count != 0 {
			t.Fatalf("db 0 %s rows after FLUSHDB = %d, want 0", table, count)
		}
		if count := joinedValueCount(t, db, 1, table); count != 1 {
			t.Fatalf("db 1 %s rows after DB 0 FLUSHDB = %d, want 1", table, count)
		}
	}

	if err := New(db).DeleteAll(); err != nil {
		t.Fatalf("DeleteAll: %v", err)
	}
	for dbIdx := 0; dbIdx < 2; dbIdx++ {
		if count := repositoryLen(t, New(db).WithDB(dbIdx)); count != 0 {
			t.Fatalf("db %d length after FLUSHALL = %d, want 0", dbIdx, count)
		}
		for _, table := range valueTables {
			if count := joinedValueCount(t, db, dbIdx, table); count != 0 {
				t.Fatalf("db %d %s rows after FLUSHALL = %d, want 0", dbIdx, table, count)
			}
		}
	}
}

func seedLogicalDB(t *testing.T, db *store.Store, dbIdx int) {
	t.Helper()

	keys := []store.RKey{
		{KDB: dbIdx, KName: "string-key", KType: 1, KVer: 1, ModifiedAt: 1, KLen: 1},
		{KDB: dbIdx, KName: "list-key", KType: 2, KVer: 1, ModifiedAt: 1, KLen: 1},
		{KDB: dbIdx, KName: "set-key", KType: 3, KVer: 1, ModifiedAt: 1, KLen: 1},
		{KDB: dbIdx, KName: "hash-key", KType: 4, KVer: 1, ModifiedAt: 1, KLen: 1},
		{KDB: dbIdx, KName: "zset-key", KType: 5, KVer: 1, ModifiedAt: 1, KLen: 1},
	}
	if err := db.DB.Create(&keys).Error; err != nil {
		t.Fatalf("seed db %d keys: %v", dbIdx, err)
	}
	values := []any{
		&store.RString{KID: keys[0].ID, KVal: []byte("value")},
		&store.RList{KID: keys[1].ID, Pos: 1, Elem: []byte("value")},
		&store.RSet{KID: keys[2].ID, Elem: []byte("value")},
		&store.RHash{KID: keys[3].ID, KField: "field", KVal: []byte("value")},
		&store.RZSet{KID: keys[4].ID, Elem: []byte("value"), Score: 1},
	}
	for _, value := range values {
		if err := db.DB.Create(value).Error; err != nil {
			t.Fatalf("seed db %d value: %v", dbIdx, err)
		}
	}
}

func repositoryLen(t *testing.T, db *DB) int {
	t.Helper()

	count, err := db.Len()
	if err != nil {
		t.Fatalf("repository length: %v", err)
	}
	return count
}

func joinedValueCount(t *testing.T, db *store.Store, dbIdx int, table string) int64 {
	t.Helper()

	var count int64
	err := db.DB.Raw(
		"SELECT count(*) FROM "+table+" v JOIN rkey k ON k.id = v.kid WHERE k.kdb = ?",
		dbIdx,
	).Scan(&count).Error
	if err != nil {
		t.Fatalf("count joined %s rows: %v", table, err)
	}
	return count
}
