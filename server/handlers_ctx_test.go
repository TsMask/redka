package server

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/command"
	"github.com/tsmask/redka/server/internal/redis"
)

// TestHandleSingleHonorsRequestContextDeadline verifies that a cancelled
// request context aborts the repository call and does not leave a half-written
// key behind. This covers R7: the timeout middleware's ctx must reach SQL.
func TestHandleSingleHonorsRequestContextDeadline(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled so the repository call fails immediately.

	state := &connState{cmds: []redis.Cmd{ctxParseCmd(t, "SET r7:deadline value")}}
	handleSingle(ctx, &ctxTestWriter{}, state, db)

	str := redis.NewRedka(context.Background(), db, 0).Str()
	if _, err := str.Get("r7:deadline"); !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("cancelled context wrote key: err=%v", err)
	}
}

// TestHandleMultiHonorsRequestContextDeadline ensures that a cancelled
// request context aborts the EXEC transaction and rolls everything back.
func TestHandleMultiHonorsRequestContextDeadline(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	state := &connState{cmds: []redis.Cmd{
		ctxParseCmd(t, "SET r7:multi:1 a"),
		ctxParseCmd(t, "SET r7:multi:2 b"),
	}}
	writer := &ctxTestWriter{}
	handleMulti(ctx, writer, state, db)

	if len(writer.errors) != 1 || writer.errors[0] != "ERR EXEC transaction failed" {
		t.Fatalf("cancelled EXEC responses = %v, want one EXEC transaction failure", writer.errors)
	}
	str := redis.NewRedka(context.Background(), db, 0).Str()
	for _, key := range []string{"r7:multi:1", "r7:multi:2"} {
		if _, err := str.Get(key); !errors.Is(err, core.ErrNotFound) {
			t.Fatalf("cancelled EXEC wrote key %s: err=%v", key, err)
		}
	}
}

// TestHandleSingleWithoutContextStillWorks guards against accidental
// over-tightening: a nil request ctx must fall back to context.Background().
func TestHandleSingleWithoutContextStillWorks(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	state := &connState{cmds: []redis.Cmd{ctxParseCmd(t, "SET r7:fallback value")}}
	// nil ctx simulates a caller that does not propagate a deadline.
	handleSingle(context.Background(), &ctxTestWriter{}, state, db)

	str := redis.NewRedka(context.Background(), db, 0).Str()
	got, err := str.Get("r7:fallback")
	if err != nil {
		t.Fatalf("fallback ctx did not write: %v", err)
	}
	if got.String() != "value" {
		t.Fatalf("fallback value = %q, want value", got.String())
	}
}

func ctxParseCmd(t *testing.T, line string) redis.Cmd {
	t.Helper()
	parts := strings.Fields(line)
	args := make([][]byte, len(parts))
	for i, p := range parts {
		args[i] = []byte(p)
	}
	cmd, err := command.Parse(args)
	if err != nil {
		t.Fatalf("parse %q: %v", line, err)
	}
	return cmd
}

type ctxTestWriter struct {
	arrays []int
	raws   [][]byte
	errors []string
}

func (w *ctxTestWriter) Context(string) any     { return nil }
func (w *ctxTestWriter) SetContext(string, any) {}
func (w *ctxTestWriter) WriteAny(any)           {}
func (w *ctxTestWriter) WriteArray(count int)   { w.arrays = append(w.arrays, count) }
func (w *ctxTestWriter) WriteBulk([]byte)       {}
func (w *ctxTestWriter) WriteBulkString(string) {}
func (w *ctxTestWriter) WriteError(msg string)  { w.errors = append(w.errors, msg) }
func (w *ctxTestWriter) WriteInt(int)           {}
func (w *ctxTestWriter) WriteInt64(int64)       {}
func (w *ctxTestWriter) WriteNull()             {}
func (w *ctxTestWriter) WriteRaw([]byte)        { w.raws = append(w.raws, []byte{}) }
func (w *ctxTestWriter) WriteString(string)     {}
func (w *ctxTestWriter) WriteUint64(uint64)     {}

var _ = ctxTestWriter{}
