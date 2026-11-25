package server

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/redis"
)

func TestWatchTrackerVersion(t *testing.T) {
	tracker := newWatchTracker()
	if got := tracker.version(0, "k"); got != 0 {
		t.Fatalf("initial version = %d, want 0", got)
	}

	tracker.addWatcher(0, "k")
	tracker.bumpKeys([]watchKey{{db: 0, key: "k"}})
	if got := tracker.version(0, "k"); got != 1 {
		t.Fatalf("version after first write = %d, want 1", got)
	}

	tracker.addWatcher(0, "k")
	tracker.bumpKeys([]watchKey{{db: 0, key: "k"}})
	if got := tracker.version(0, "k"); got != 2 {
		t.Fatalf("version after second write = %d, want 2", got)
	}

	tracker.removeWatcher(0, "k")
	tracker.removeWatcher(0, "k")
	if got := tracker.watchCountForTest(); got != 0 {
		t.Fatalf("watcher count after remove = %d, want 0", got)
	}
}

func TestConnStateWatchDirty(t *testing.T) {
	tracker := newWatchTracker()
	state := &connState{}
	state.addWatch(0, "k", watchBaseline{version: 0, kver: 3})
	tracker.addWatcher(0, "k")

	current := map[watchKey]uint64{{db: 0, key: "k"}: 3}
	if state.watchDirty(tracker, current) {
		t.Fatal("clean watch reported dirty")
	}
	tracker.bumpKeys([]watchKey{{db: 0, key: "k"}})
	if !state.watchDirty(tracker, current) {
		t.Fatal("modified watch was not reported dirty")
	}

	state.clearWatches(tracker)
	if state.watchDirty(tracker, current) {
		t.Fatal("cleared watch reported dirty")
	}

	state.addWatch(0, "k", watchBaseline{version: 1, kver: 3})
	current[watchKey{db: 0, key: "k"}] = 4
	if !state.watchDirty(tracker, current) {
		t.Fatal("store version change was not reported dirty")
	}
}

func TestWatchTouchFor(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		result any
		want   []string
	}{
		{name: "set", args: []string{"k", "v"}, result: true, want: []string{"k"}},
		{name: "set same value", args: []string{"k", "v"}, result: true, want: []string{"k"}},
		{name: "setnx no-op", args: []string{"k", "v"}, result: false},
		{name: "setnx create", args: []string{"k", "v"}, result: true, want: []string{"k"}},
		{name: "expire", args: []string{"k"}, result: true, want: []string{"k"}},
		{name: "persist no-op", args: []string{"k"}, result: false},
		{name: "mset", args: []string{"a", "1", "b", "2"}, result: true, want: []string{"a", "b"}},
		{name: "hset", args: []string{"k", "f", "v"}, result: 0, want: []string{"k"}},
		{name: "sadd no-op", args: []string{"k", "a"}, result: 0},
		{name: "sadd new", args: []string{"k", "a"}, result: 1, want: []string{"k"}},
		{name: "zadd", args: []string{"k", "1", "a"}, result: 0, want: []string{"k"}},
		{name: "rename same", args: []string{"k", "k"}, result: true},
		{name: "rename other", args: []string{"a", "b"}, result: true, want: []string{"a", "b"}},
		{name: "ltrim missing", args: []string{"k"}, result: false},
		{name: "ltrim existing", args: []string{"k"}, result: true, want: []string{"k"}},
		{name: "setrange empty", args: []string{"k", "0", ""}, result: 0},
		{name: "setrange value", args: []string{"k", "0", "x"}, result: 1, want: []string{"k"}},
		{name: "zincrby zero", args: []string{"k", "0", "a"}, result: 0.0},
		{name: "zincrby delta", args: []string{"k", "1", "a"}, result: 1.0, want: []string{"k"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := make([][]byte, len(test.args))
			for i, arg := range test.args {
				args[i] = []byte(arg)
			}
			name := strings.Fields(test.name)[0]
			touch := watchTouchFor(0, name, args, test.result)
			if len(touch.keys) != len(test.want) {
				t.Fatalf("touch keys = %v, want %v", touch.keys, test.want)
			}
			for i, key := range touch.keys {
				if key.key != test.want[i] {
					t.Fatalf("touch key %d = %q, want %q", i, key.key, test.want[i])
				}
			}
		})
	}
}

func TestSetWatchTouchConditional(t *testing.T) {
	tests := []struct {
		name   string
		args   []string
		result any
		want   bool
	}{
		{name: "nx conflict", args: []string{"k", "v", "NX"}, result: false},
		{name: "nx create", args: []string{"k", "v", "NX"}, result: true, want: true},
		{name: "xx missing", args: []string{"k", "v", "XX"}, result: false},
		{name: "xx update", args: []string{"k", "v", "XX"}, result: true, want: true},
		{name: "nx get conflict", args: []string{"k", "v", "NX", "GET"}, result: redisValue("old")},
		{name: "nx get create", args: []string{"k", "v", "NX", "GET"}, result: nil, want: true},
		{name: "xx get missing", args: []string{"k", "v", "XX", "GET"}, result: nil},
		{name: "xx get update", args: []string{"k", "v", "XX", "GET"}, result: redisValue("old"), want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := make([][]byte, len(test.args))
			for i, arg := range test.args {
				args[i] = []byte(arg)
			}
			touch := setWatchTouch(0, args, test.result)
			if got := len(touch.keys) == 1; got != test.want {
				t.Fatalf("touched = %v, want %v (keys=%v)", got, test.want, touch.keys)
			}
		})
	}
}

func TestZIncrByTouchAfter(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		existed bool
		want    bool
	}{
		{name: "existing zero", args: []string{"k", "0", "a"}, existed: true},
		{name: "existing delta", args: []string{"k", "1", "a"}, existed: true, want: true},
		{name: "new zero", args: []string{"k", "0", "a"}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := make([][]byte, len(test.args))
			for i, arg := range test.args {
				args[i] = []byte(arg)
			}
			touch := zincrbyTouchAfter(0, args, test.existed)
			if got := len(touch.keys) == 1; got != test.want {
				t.Fatalf("touched = %v, want %v (keys=%v)", got, test.want, touch.keys)
			}
		})
	}
}

func TestKeyVersions(t *testing.T) {
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer db.Close()

	red := redis.NewRedka(context.Background(), db, 0)
	if err := red.Str().Set("exists", "1"); err != nil {
		t.Fatalf("set key: %v", err)
	}
	if err := red.Str().Set("expired", "1"); err != nil {
		t.Fatalf("set expiring key: %v", err)
	}
	if err := red.Key().Expire("expired", -time.Second); err != nil {
		t.Fatalf("expire key: %v", err)
	}

	keys := []watchKey{
		{db: 0, key: "exists"},
		{db: 0, key: "expired"},
		{db: 0, key: "missing"},
	}
	versions, err := keyVersions(context.Background(), db, keys)
	if err != nil {
		t.Fatalf("key versions: %v", err)
	}

	if versions[watchKey{db: 0, key: "exists"}] == 0 {
		t.Fatal("existing key has version 0")
	}
	if versions[watchKey{db: 0, key: "expired"}] != 0 {
		t.Fatalf("expired key version = %d, want 0", versions[watchKey{db: 0, key: "expired"}])
	}
	if versions[watchKey{db: 0, key: "missing"}] != 0 {
		t.Fatalf("missing key version = %d, want 0", versions[watchKey{db: 0, key: "missing"}])
	}
}

type respReply struct {
	kind byte
	str  string
	num  int64
	arr  []respReply
	null bool
}

type respClient struct {
	conn   net.Conn
	reader *bufio.Reader
}

func newWatchTestServer(t *testing.T) (addr string, stop func()) {
	t.Helper()
	db, err := store.Open(":memory:")
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	srv := New("tcp", "127.0.0.1:0", db)
	ready := make(chan error, 1)
	go func() {
		_ = srv.Start(ready)
	}()
	if err := <-ready; err != nil {
		db.Close()
		t.Fatalf("start server: %v", err)
	}
	return srv.Addr().String(), func() {
		_ = srv.Stop()
		_ = db.Close()
	}
}

func dialResp(t *testing.T, addr string) *respClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	return &respClient{conn: conn, reader: bufio.NewReader(conn)}
}

func (c *respClient) close() {
	_ = c.conn.Close()
}

func (c *respClient) do(t *testing.T, args ...string) respReply {
	t.Helper()
	var request strings.Builder
	fmt.Fprintf(&request, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&request, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := c.conn.Write([]byte(request.String())); err != nil {
		t.Fatalf("write command: %v", err)
	}
	return c.readReply(t)
}

func (c *respClient) readReply(t *testing.T) respReply {
	t.Helper()
	line, err := c.reader.ReadString('\n')
	if err != nil {
		t.Fatalf("read reply: %v", err)
	}
	line = strings.TrimRight(line, "\r\n")
	if line == "" {
		t.Fatal("empty reply line")
	}
	kind := line[0]
	switch kind {
	case '+', '-', ':':
		value := line[1:]
		reply := respReply{kind: kind, str: value}
		if kind == ':' {
			fmt.Sscanf(value, "%d", &reply.num)
		}
		return reply
	case '$':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return respReply{kind: kind, null: true}
		}
		data := make([]byte, n+2)
		if _, err := c.reader.Read(data); err != nil {
			t.Fatalf("read bulk: %v", err)
		}
		return respReply{kind: kind, str: string(data[:n])}
	case '*':
		var n int
		fmt.Sscanf(line[1:], "%d", &n)
		if n < 0 {
			return respReply{kind: kind, null: true}
		}
		reply := respReply{kind: kind, arr: make([]respReply, 0, n)}
		for i := 0; i < n; i++ {
			reply.arr = append(reply.arr, c.readReply(t))
		}
		return reply
	default:
		t.Fatalf("unexpected reply kind %q in %q", kind, line)
		return respReply{}
	}
}

func redisValue(value string) coreValue {
	return coreValue(value)
}

// coreValue is a small alias used to exercise the core.Value branch in tests
// without importing the internal core package into the test helper.
type coreValue []byte

func requireOK(t *testing.T, reply respReply) {
	t.Helper()
	if reply.kind != '+' || reply.str != "OK" {
		t.Fatalf("reply = %#v, want +OK", reply)
	}
}

func requireQueued(t *testing.T, reply respReply) {
	t.Helper()
	if reply.kind != '+' || reply.str != "QUEUED" {
		t.Fatalf("reply = %#v, want +QUEUED", reply)
	}
}

func requireInt(t *testing.T, reply respReply, want int64) {
	t.Helper()
	if reply.kind != ':' || reply.num != want {
		t.Fatalf("reply = %#v, want :%d", reply, want)
	}
}

func requireBulk(t *testing.T, reply respReply, want string) {
	t.Helper()
	if reply.kind != '$' || reply.null || reply.str != want {
		t.Fatalf("reply = %#v, want bulk %q", reply, want)
	}
}

func requireNullBulk(t *testing.T, reply respReply) {
	t.Helper()
	if reply.kind != '$' || !reply.null {
		t.Fatalf("reply = %#v, want null bulk", reply)
	}
}

func requireError(t *testing.T, reply respReply, substring string) {
	t.Helper()
	if reply.kind != '-' || !strings.Contains(reply.str, substring) {
		t.Fatalf("reply = %#v, want error containing %q", reply, substring)
	}
}

func requireNullArray(t *testing.T, reply respReply) {
	t.Helper()
	if reply.kind != '*' || !reply.null {
		t.Fatalf("reply = %#v, want null array", reply)
	}
}

func requireArray(t *testing.T, reply respReply, want ...string) {
	t.Helper()
	if reply.kind != '*' || reply.null || len(reply.arr) != len(want) {
		t.Fatalf("reply = %#v, want array %v", reply, want)
	}
	for i, expected := range want {
		if reply.arr[i].kind == '$' && reply.arr[i].null {
			if expected != "<nil>" {
				t.Fatalf("reply[%d] = nil, want %q", i, expected)
			}
			continue
		}
		if reply.arr[i].str != expected {
			t.Fatalf("reply[%d] = %q, want %q", i, reply.arr[i].str, expected)
		}
	}
}

func TestWatchAbortOnModifiedKey(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, b.do(t, "SET", "k", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
	requireBulk(t, a.do(t, "GET", "k"), "2")
}

func TestWatchCleanTransaction(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "SET", "k", "3"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "OK", "3")
}

func TestWatchSameValueWriteInvalidates(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	// Redis signals the write even when the stored value does not change.
	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, b.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestWatchMultipleKeys(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k1", "1"))
	requireOK(t, a.do(t, "SET", "k2", "1"))
	requireOK(t, a.do(t, "WATCH", "k1", "k2"))
	requireOK(t, b.do(t, "SET", "k2", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k1"))
	requireQueued(t, a.do(t, "GET", "k2"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestUnwatchClearsWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "UNWATCH"))
	requireOK(t, b.do(t, "SET", "k", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "2")
}

func TestWatchInsideMulti(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "WATCH", "k"), "WATCH inside MULTI")
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "1")
}

func TestUnwatchInsideMulti(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "UNWATCH"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "OK", "1")

	requireOK(t, b.do(t, "SET", "k", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "2")
}

func TestDiscardClearsWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireOK(t, a.do(t, "DISCARD"))
	requireOK(t, b.do(t, "SET", "k", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "2")
}

func TestWatchMissingKeyCreated(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestWatchNoOpWrites(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	if reply := b.do(t, "SETNX", "k", "2"); reply.kind != ':' || reply.num != 0 {
		t.Fatalf("SETNX reply = %#v, want :0", reply)
	}
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "1")

	requireInt(t, a.do(t, "SADD", "s", "a"), 1)
	requireOK(t, a.do(t, "WATCH", "s"))
	if reply := b.do(t, "SADD", "s", "a"); reply.kind != ':' || reply.num != 0 {
		t.Fatalf("SADD reply = %#v, want :0", reply)
	}
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "SCARD", "s"))
	requireArray(t, a.do(t, "EXEC"), "1")
}

func TestWatchWrongArgs(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireError(t, a.do(t, "WATCH"), "wrong number of arguments for 'watch' command")
	requireError(t, a.do(t, "UNWATCH", "x"), "wrong number of arguments for 'unwatch' command")
	requireError(t, a.do(t, "EXEC", "x"),
		"EXECABORT Transaction discarded because of: "+
			"wrong number of arguments for 'exec' command")
	requireError(t, a.do(t, "DISCARD", "x"),
		"wrong number of arguments for 'discard' command")
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "WATCH"), "wrong number of arguments for 'watch' command")
	requireOK(t, a.do(t, "DISCARD"))
}

func TestClientListWatchCount(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "CLIENT", "SETNAME", "watcher"))
	requireOK(t, a.do(t, "WATCH", "k1", "k2"))
	list := a.do(t, "CLIENT", "LIST")
	if list.kind != '$' || !strings.Contains(list.str, "name=watcher") ||
		!strings.Contains(list.str, "watch=2") {
		t.Fatalf("CLIENT LIST = %q, want watch=2", list.str)
	}

	requireOK(t, a.do(t, "UNWATCH"))
	list = a.do(t, "CLIENT", "LIST")
	if list.kind != '$' || !strings.Contains(list.str, "name=watcher") ||
		!strings.Contains(list.str, "watch=0") {
		t.Fatalf("CLIENT LIST = %q, want watch=0", list.str)
	}
}

func TestFlushCommandsInvalidateExistingWatchedKeys(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "WATCH", "missing"))
	requireOK(t, a.do(t, "FLUSHDB"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "missing"))
	requireArray(t, a.do(t, "EXEC"), "<nil>")

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "FLUSHDB"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestZAddScoreUpdateInvalidatesWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireInt(t, a.do(t, "ZADD", "k", "1", "a"), 1)
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "ZADD", "k", "1", "a"), 0)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "ZCARD", "k"))
	requireArray(t, a.do(t, "EXEC"), "1")

	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "ZADD", "k", "2", "a"), 0)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "ZCARD", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestPersistWithoutTTLDoesNotInvalidateWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	if reply := a.do(t, "PERSIST", "k"); reply.kind != ':' || reply.num != 0 {
		t.Fatalf("PERSIST reply = %#v, want :0", reply)
	}
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "1")
}

func TestDelOnlyInvalidatesExistingWatchedKeys(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	// Deleting an unrelated key does not touch a watched missing key.
	requireOK(t, a.do(t, "SET", "other", "1"))
	requireOK(t, a.do(t, "WATCH", "missing"))
	requireInt(t, a.do(t, "DEL", "missing", "other"), 1)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "missing"))
	requireArray(t, a.do(t, "EXEC"), "<nil>")

	// Deleting an existing watched key does invalidate it.
	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "DEL", "k"), 1)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestLTrimMissingKeyDoesNotInvalidateWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "WATCH", "missing"))
	requireOK(t, a.do(t, "LTRIM", "missing", "0", "-1"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "LLEN", "missing"))
	requireArray(t, a.do(t, "EXEC"), "0")
}

func TestLTrimExistingKeyInvalidatesWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	// Keeping the whole list is still a write for WATCH purposes.
	requireInt(t, a.do(t, "RPUSH", "k", "a", "b", "c"), 3)
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "LTRIM", "k", "0", "-1"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "LLEN", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestSetRangeEmptyValueDoesNotInvalidateWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "abc"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "SETRANGE", "k", "0", ""), 3)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "abc")

	requireOK(t, a.do(t, "WATCH", "missing"))
	requireInt(t, a.do(t, "SETRANGE", "missing", "0", ""), 0)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "missing"))
	requireArray(t, a.do(t, "EXEC"), "<nil>")
}

func TestZIncrByZeroOnExistingMemberDoesNotInvalidateWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireInt(t, a.do(t, "ZADD", "k", "0", "a"), 1)
	requireOK(t, a.do(t, "WATCH", "k"))
	requireBulk(t, a.do(t, "ZINCRBY", "k", "0", "a"), "0")
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "ZSCORE", "k", "a"))
	requireArray(t, a.do(t, "EXEC"), "0")

	// A zero increment on a missing member creates it and invalidates WATCH.
	requireOK(t, a.do(t, "WATCH", "k"))
	requireBulk(t, a.do(t, "ZINCRBY", "k", "0", "new"), "0")
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "ZCARD", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestDelThenZIncrByNewMemberInvalidatesWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	// The key is deleted and recreated, so its kver returns to the baseline.
	// The new member must still invalidate WATCH.
	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "DEL", "k"), 1)
	requireBulk(t, a.do(t, "ZINCRBY", "k", "0", "new"), "0")
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "ZCARD", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestWatchKeyExpiryAbortsTransaction(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1", "PX", "50"))
	requireOK(t, a.do(t, "WATCH", "k"))
	time.Sleep(100 * time.Millisecond)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestWatchAlreadyExpiredKeyDoesNotAbort(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1", "PX", "50"))
	time.Sleep(100 * time.Millisecond)
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "<nil>")
}

func TestWatchDifferentDatabases(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()
	b := dialResp(t, addr)
	defer b.close()

	// Watch k in db 0, then work in db 1.
	requireOK(t, a.do(t, "SELECT", "0"))
	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "SELECT", "1"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "<nil>")

	// A write to k in db 1 does not touch the watched key in db 0.
	requireOK(t, a.do(t, "SELECT", "0"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "SELECT", "1"))
	requireOK(t, b.do(t, "SELECT", "1"))
	requireOK(t, b.do(t, "SET", "k", "2"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "2")

	// A write to k in db 0 does touch the watched key.
	requireOK(t, a.do(t, "SELECT", "0"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "SELECT", "1"))
	requireOK(t, b.do(t, "SELECT", "0"))
	requireOK(t, b.do(t, "SET", "k", "3"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestRenameSameKeyDoesNotInvalidateWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireOK(t, a.do(t, "RENAME", "k", "k"))
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireArray(t, a.do(t, "EXEC"), "1")
}

func TestExpireInvalidatesWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "EXPIRE", "k", "100"), 1)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestPersistWithTTLInvalidatesWatch(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	requireOK(t, a.do(t, "SET", "k", "1", "EX", "100"))
	requireOK(t, a.do(t, "WATCH", "k"))
	requireInt(t, a.do(t, "PERSIST", "k"), 1)
	requireOK(t, a.do(t, "MULTI"))
	requireQueued(t, a.do(t, "GET", "k"))
	requireNullArray(t, a.do(t, "EXEC"))
}

func TestMultiQueueingErrorsAbortExec(t *testing.T) {
	addr, stop := newWatchTestServer(t)
	defer stop()

	a := dialResp(t, addr)
	defer a.close()

	// An unknown command aborts the transaction at queue time.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "BOGUS"), "unknown command")
	requireQueued(t, a.do(t, "SET", "k", "1"))
	requireError(t, a.do(t, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")
	requireNullBulk(t, a.do(t, "GET", "k"))

	// A wrong-arity command aborts the transaction at queue time.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "SET", "k"), "wrong number of arguments")
	requireQueued(t, a.do(t, "SET", "k", "1"))
	requireError(t, a.do(t, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")

	// DISCARD with extra arguments also marks the transaction dirty.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "DISCARD", "x"),
		"wrong number of arguments for 'discard' command")
	requireQueued(t, a.do(t, "SET", "k", "1"))
	requireError(t, a.do(t, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")

	// MULTI with extra arguments reports arity, not nesting.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "MULTI", "x"),
		"wrong number of arguments for 'multi' command")
	requireQueued(t, a.do(t, "SET", "k", "1"))
	requireError(t, a.do(t, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")

	// UNWATCH with extra arguments marks the transaction dirty.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "UNWATCH", "x"),
		"wrong number of arguments for 'unwatch' command")
	requireQueued(t, a.do(t, "SET", "k", "1"))
	requireError(t, a.do(t, "EXEC"),
		"EXECABORT Transaction discarded because of previous errors.")

	// A correctly formed nested MULTI is rejected but does not dirty the queue.
	requireOK(t, a.do(t, "MULTI"))
	requireError(t, a.do(t, "MULTI"), "MULTI calls can not be nested")
	requireQueued(t, a.do(t, "SET", "k", "2"))
	requireArray(t, a.do(t, "EXEC"), "OK")
	requireBulk(t, a.do(t, "GET", "k"), "2")
}

func (t *watchTracker) watchCountForTest() int {
	return int(t.active.Load())
}
