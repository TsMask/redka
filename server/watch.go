package server

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/redis"
)

// watchKey identifies a key in a logical database.
type watchKey struct {
	db  int
	key string
}

// watchTracker keeps a monotonically increasing version for every key that is
// currently watched. Writes to unwatched keys do not need a version update:
// a future WATCH starts from the current state.
type watchTracker struct {
	mu       sync.RWMutex
	versions map[watchKey]uint64
	watchers map[watchKey]int
	active   atomic.Int64
}

// watchKeyChunk bounds the number of keys in one kver lookup.
const watchKeyChunk = 500

func newWatchTracker() *watchTracker {
	return &watchTracker{
		versions: make(map[watchKey]uint64),
		watchers: make(map[watchKey]int),
	}
}

func (t *watchTracker) version(db int, key string) uint64 {
	if t == nil {
		return 0
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.versions[watchKey{db: db, key: key}]
}

func (t *watchTracker) addWatcher(db int, key string) {
	if t == nil {
		return
	}
	k := watchKey{db: db, key: key}
	t.mu.Lock()
	if t.watchers == nil {
		t.watchers = make(map[watchKey]int)
	}
	if t.versions == nil {
		t.versions = make(map[watchKey]uint64)
	}
	t.watchers[k]++
	t.active.Add(1)
	t.mu.Unlock()
}

func (t *watchTracker) removeWatcher(db int, key string) {
	if t == nil {
		return
	}
	k := watchKey{db: db, key: key}
	t.mu.Lock()
	if t.watchers[k] > 0 {
		t.watchers[k]--
		if t.watchers[k] == 0 {
			delete(t.watchers, k)
			delete(t.versions, k)
		}
		t.active.Add(-1)
	}
	t.mu.Unlock()
}

// bumpKeys advances the version of every watched key in keys.
func (t *watchTracker) bumpKeys(keys []watchKey) {
	if t == nil || len(keys) == 0 || t.active.Load() == 0 {
		return
	}
	t.mu.Lock()
	for _, key := range keys {
		if t.watchers[key] > 0 {
			t.versions[key]++
		}
	}
	t.mu.Unlock()
}

// watchTouch describes the watched keys changed by one command.
type watchTouch struct {
	keys []watchKey
}

// watchTouchFor derives the keys that a successfully completed command may
// have modified. Commands that report a no-op (for example SETNX on an
// existing key) return an empty touch.
func watchTouchFor(db int, name string, args [][]byte, result any) watchTouch {
	key := func(index int) watchTouch {
		if index < 0 || index >= len(args) {
			return watchTouch{}
		}
		return watchTouch{keys: []watchKey{{db: db, key: string(args[index])}}}
	}
	evenKeys := func() watchTouch {
		var keys []watchKey
		for i := 0; i < len(args); i += 2 {
			keys = append(keys, watchKey{db: db, key: string(args[i])})
		}
		return watchTouch{keys: keys}
	}

	switch name {
	case "expire", "pexpire", "expireat", "pexpireat", "persist":
		if resultBool(result) {
			return key(0)
		}

	case "rename":
		if resultBool(result) && len(args) >= 2 && string(args[0]) != string(args[1]) {
			return watchTouch{keys: []watchKey{
				{db: db, key: string(args[0])},
				{db: db, key: string(args[1])},
			}}
		}

	case "renamenx":
		if resultBool(result) && len(args) >= 2 {
			return watchTouch{keys: []watchKey{
				{db: db, key: string(args[0])},
				{db: db, key: string(args[1])},
			}}
		}

	case "set":
		return setWatchTouch(db, args, result)

	case "setex", "psetex":
		return key(0)

	case "setnx":
		if resultBool(result) {
			return key(0)
		}

	case "mset":
		return evenKeys()

	case "append", "getset",
		"incr", "decr", "incrby", "decrby", "incrbyfloat":
		return key(0)

	case "setrange":
		// An empty value is a no-op in Redis, even when the key exists.
		if len(args) >= 3 && len(args[2]) > 0 {
			return key(0)
		}

	case "hset", "hmset", "hincrby", "hincrbyfloat":
		return key(0)

	case "hsetnx":
		if resultBool(result) {
			return key(0)
		}

	case "hdel":
		if resultInt(result) > 0 {
			return key(0)
		}

	case "sadd", "srem":
		if resultInt(result) > 0 {
			return key(0)
		}

	case "spop":
		if !isNilResult(result) {
			return key(0)
		}

	case "smove":
		if resultInt(result) > 0 && len(args) >= 2 && string(args[0]) != string(args[1]) {
			return watchTouch{keys: []watchKey{
				{db: db, key: string(args[0])},
				{db: db, key: string(args[1])},
			}}
		}

	case "sdiffstore", "sinterstore", "sunionstore",
		"zinterstore", "zunionstore":
		// Store commands always rewrite the destination, even when the result
		// happens to be identical.
		return key(0)

	case "zadd":
		// ZADD returns the number of new members, so an updated score is
		// indistinguishable from a no-op by the return value alone.
		return key(0)

	case "zincrby":
		delta, err := strconv.ParseFloat(string(args[1]), 64)
		// A zero increment leaves an existing member unchanged. A new member
		// is detected by the store version changing from zero.
		if err != nil || delta != 0 {
			return key(0)
		}

	case "zrem", "zremrangebyrank", "zremrangebyscore":
		if resultInt(result) > 0 {
			return key(0)
		}

	case "lpush", "rpush", "lset":
		return key(0)

	case "ltrim":
		// LTRIM reports OK even when the key is missing. Redis only signals
		// the key when it existed before the command.
		if resultBool(result) {
			return key(0)
		}

	case "lpop", "rpop":
		if !isNilResult(result) {
			return key(0)
		}

	case "lrem", "linsert":
		if resultInt(result) > 0 {
			return key(0)
		}

	case "rpoplpush":
		if !isNilResult(result) && len(args) >= 2 {
			return watchTouch{keys: []watchKey{
				{db: db, key: string(args[0])},
				{db: db, key: string(args[1])},
			}}
		}
	}

	return watchTouch{}
}

func setWatchTouch(db int, args [][]byte, result any) watchTouch {
	if len(args) == 0 {
		return watchTouch{}
	}
	key := watchKey{db: db, key: string(args[0])}
	nx, xx, get := setOptions(args)
	if !nx && !xx {
		return watchTouch{keys: []watchKey{key}}
	}
	if !get {
		if resultBool(result) {
			return watchTouch{keys: []watchKey{key}}
		}
		return watchTouch{}
	}

	// With GET, the result is the previous value. For NX, a nil previous
	// value means the key was created. For XX, a non-nil previous value means
	// the existing key was updated.
	if nx && isNilResult(result) {
		return watchTouch{keys: []watchKey{key}}
	}
	if xx && !isNilResult(result) {
		return watchTouch{keys: []watchKey{key}}
	}
	return watchTouch{}
}

func setOptions(args [][]byte) (nx, xx, get bool) {
	for i := 2; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "nx":
			nx = true
		case "xx":
			xx = true
		case "get":
			get = true
		}
	}
	return nx, xx, get
}

func resultBool(result any) bool {
	value, _ := result.(bool)
	return value
}

func resultInt(result any) int {
	switch value := result.(type) {
	case int:
		return value
	case int32:
		return int(value)
	case int64:
		return int(value)
	}
	return 0
}

func isNilResult(result any) bool {
	if result == nil {
		return true
	}
	switch value := result.(type) {
	case core.Value:
		return value == nil
	case []byte:
		return value == nil
	}
	return false
}

func zaddKeyMembers(args [][]byte) (string, []string, bool) {
	if len(args) < 3 || len(args)%2 == 0 {
		return "", nil, false
	}
	members := make([]string, 0, (len(args)-1)/2)
	for i := 1; i < len(args); i += 2 {
		members = append(members, string(args[i+1]))
	}
	return string(args[0]), members, true
}

// zaddScoresBefore captures the scores that a ZADD command may change. ZADD
// returns only the number of new members, so this snapshot distinguishes an
// updated score from a no-op.
func zaddScoresBefore(red redis.Redka, args [][]byte) map[string]float64 {
	key, members, ok := zaddKeyMembers(args)
	if !ok {
		return nil
	}
	scores := make(map[string]float64, len(members))
	for _, member := range members {
		score, err := red.ZSet().GetScore(key, member)
		if err == nil {
			scores[member] = score
		}
	}
	return scores
}

func zaddTouchAfter(
	db int, args [][]byte, before map[string]float64, red redis.Redka,
) watchTouch {
	key, members, ok := zaddKeyMembers(args)
	if !ok {
		return watchTouch{}
	}
	for _, member := range members {
		score, err := red.ZSet().GetScore(key, member)
		if err != nil {
			continue
		}
		old, existed := before[member]
		if !existed || old != score {
			return watchTouch{keys: []watchKey{{db: db, key: key}}}
		}
	}
	return watchTouch{}
}

// zincrbyExistedBefore reports whether the member exists before ZINCRBY. A
// zero increment on a new member still creates the key, which the return
// value alone cannot distinguish from an unchanged existing member.
func zincrbyExistedBefore(red redis.Redka, args [][]byte) bool {
	if len(args) < 3 {
		return false
	}
	_, err := red.ZSet().GetScore(string(args[0]), string(args[2]))
	return err == nil
}

func zincrbyTouchAfter(db int, args [][]byte, existed bool) watchTouch {
	if len(args) < 3 {
		return watchTouch{}
	}
	if !existed {
		return watchTouch{keys: []watchKey{{db: db, key: string(args[0])}}}
	}
	delta, err := strconv.ParseFloat(string(args[1]), 64)
	if err != nil || delta != 0 {
		return watchTouch{keys: []watchKey{{db: db, key: string(args[0])}}}
	}
	return watchTouch{}
}

// keyVersions returns the stored kver for each key. Missing and expired keys
// have version 0. Keys are grouped by database and queried in chunks to stay
// below the SQL parameter limit.
func keyVersions(
	ctx context.Context, s *store.Store, keys []watchKey,
) (map[watchKey]uint64, error) {
	versions := make(map[watchKey]uint64, len(keys))
	if s == nil || len(keys) == 0 {
		return versions, nil
	}

	byDB := make(map[int][]watchKey)
	for _, key := range keys {
		byDB[key.db] = append(byDB[key.db], key)
	}

	now := time.Now().UnixMilli()
	for dbIdx, dbKeys := range byDB {
		names := make([]string, len(dbKeys))
		for i, key := range dbKeys {
			names[i] = key.key
		}
		for start := 0; start < len(names); start += watchKeyChunk {
			end := start + watchKeyChunk
			if end > len(names) {
				end = len(names)
			}
			var rows []struct {
				KName string `gorm:"column:kname"`
				KVer  int    `gorm:"column:kver"`
			}
			err := s.WithContext(ctx).DB.Model(&store.RKey{}).
				Select("kname", "kver").
				Where("kdb = ? AND kname IN ?", dbIdx, names[start:end]).
				Scopes(store.NotExpired(now)).
				Scan(&rows).Error
			if err != nil {
				return nil, err
			}
			for _, row := range rows {
				// Keep existing keys above zero even if an old row has kver 0,
				// so a delete can never alias a missing key.
				versions[watchKey{db: dbIdx, key: row.KName}] = uint64(row.KVer) + 1
			}
		}
	}
	return versions, nil
}

// watchBaselines captures a consistent WATCH baseline. Tracker versions and
// stored kver values are read twice, with the other read between them. If a
// concurrent write lands between the reads, the baseline is marked dirty so
// EXEC aborts instead of missing the write.
func watchBaselines(
	ctx context.Context, s *store.Store, tracker *watchTracker, keys []watchKey,
) (map[watchKey]watchBaseline, error) {
	version1 := make(map[watchKey]uint64, len(keys))
	for _, key := range keys {
		version1[key] = tracker.version(key.db, key.key)
	}
	kver1, err := keyVersions(ctx, s, keys)
	if err != nil {
		return nil, err
	}

	version2 := make(map[watchKey]uint64, len(keys))
	for _, key := range keys {
		version2[key] = tracker.version(key.db, key.key)
	}
	kver2, err := keyVersions(ctx, s, keys)
	if err != nil {
		return nil, err
	}

	baselines := make(map[watchKey]watchBaseline, len(keys))
	for _, key := range keys {
		baselines[key] = watchBaseline{
			version: version1[key],
			kver:    kver1[key],
			dirty:   version1[key] != version2[key] || kver1[key] != kver2[key],
		}
	}
	return baselines, nil
}

func applyWatchTouch(tracker *watchTracker, touch watchTouch) {
	tracker.bumpKeys(touch.keys)
}

func applyWatchTouches(tracker *watchTracker, touches []watchTouch) {
	for _, touch := range touches {
		applyWatchTouch(tracker, touch)
	}
}
