package server

import (
	"errors"
	"fmt"
	"sync"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/server/internal/redis"
)

const (
	// maxQueued is the maximum number of commands that can be queued
	// in a MULTI transaction. Prevents memory exhaustion from unbounded
	// QUEUED commands without EXEC or DISCARD.
	maxQueued = 4096
)

var errQueueFull = errors.New("ERR transaction queue full")

// ctxKeyState is the context map key for connection state.
const ctxKeyState = "state"

// getState returns the connection state.
func getState(conn redcon.Conn) *connState {
	ctx := connContext(conn)
	if v, ok := ctx.Load(ctxKeyState); ok {
		if s, ok := v.(*connState); ok {
			return s
		}
	}
	state := new(connState)
	ctx.Store(ctxKeyState, state)
	return state
}

// connContext returns the sync.Map from the connection context,
// creating and setting a new one if necessary.
func connContext(conn redcon.Conn) *sync.Map {
	if existing := conn.Context(); existing != nil {
		if m, ok := existing.(*sync.Map); ok {
			return m
		}
	}
	m := &sync.Map{}
	conn.SetContext(m)
	return m
}

// connDBIndex returns the logical database selected by the connection.
func connDBIndex(conn redcon.Conn) int {
	ctx := connContext(conn)
	if v, ok := ctx.Load(redis.CtxKeySelectedDB); ok {
		if idx, ok := v.(int); ok {
			return idx
		}
	}
	return 0
}

// updateWatchCount mirrors the connection's watch count into CLIENT LIST.
func updateWatchCount(conn redcon.Conn, count int) {
	ctx := connContext(conn)
	registry, _ := ctx.Load(redis.CtxKeyClientRegistry)
	clientRegistry, _ := registry.(*redis.ClientRegistry)
	if clientRegistry == nil {
		return
	}
	id, _ := ctx.Load(redis.CtxKeyConnID)
	clientID, _ := id.(int64)
	if clientID == 0 {
		return
	}
	clientRegistry.Update(clientID, func(info *redis.ClientInfo) {
		info.Watch = count
	})
}

// connState represents the connection state.
type connState struct {
	inMulti   bool
	dirtyExec bool
	cmds      []redis.Cmd
	watched   map[watchKey]watchBaseline
}

// watchBaseline records the state of a key when WATCH was called. The store
// version covers deletions, expiration, and writes that do not pass through
// the in-memory tracker.
type watchBaseline struct {
	version uint64
	kver    uint64
	dirty   bool
}

// hasWatch reports whether the key is already watched by this connection.
func (s *connState) hasWatch(db int, key string) bool {
	_, ok := s.watched[watchKey{db: db, key: key}]
	return ok
}

// addWatch starts watching a key at the given baseline. It does not refresh
// the baseline for a key that is already watched.
func (s *connState) addWatch(db int, key string, baseline watchBaseline) {
	if s.watched == nil {
		s.watched = make(map[watchKey]watchBaseline)
	}
	s.watched[watchKey{db: db, key: key}] = baseline
}

// watchCount returns the number of keys watched by this connection.
func (s *connState) watchCount() int {
	return len(s.watched)
}

// watchDirty reports whether any watched key changed since WATCH. The current
// store versions are read by the caller; keys absent from the map have kver 0.
func (s *connState) watchDirty(
	tracker *watchTracker, current map[watchKey]uint64,
) bool {
	for key, baseline := range s.watched {
		if baseline.dirty {
			return true
		}
		if tracker.version(key.db, key.key) != baseline.version {
			return true
		}
		if current[key] != baseline.kver {
			return true
		}
	}
	return false
}

// watchKeys returns the keys currently watched by this connection.
func (s *connState) watchKeys() []watchKey {
	keys := make([]watchKey, 0, len(s.watched))
	for key := range s.watched {
		keys = append(keys, key)
	}
	return keys
}

// clearWatches stops watching every key held by this connection.
func (s *connState) clearWatches(tracker *watchTracker) {
	for key := range s.watched {
		tracker.removeWatcher(key.db, key.key)
	}
	s.watched = nil
}

// push adds a command to the state.
// Returns errQueueFull if the transaction queue exceeds maxQueued.
func (s *connState) push(cmd redis.Cmd) error {
	if len(s.cmds) >= maxQueued {
		return errQueueFull
	}
	s.cmds = append(s.cmds, cmd)
	return nil
}

// pop removes the last command from the state and returns it.
func (s *connState) pop() redis.Cmd {
	if len(s.cmds) == 0 {
		return nil
	}
	var last redis.Cmd
	s.cmds, last = s.cmds[:len(s.cmds)-1], s.cmds[len(s.cmds)-1]
	return last
}

// clear removes all commands from the state and releases the backing array.
// Truncating to length 0 allows the GC to reclaim the backing array.
func (s *connState) clear() {
	s.cmds = s.cmds[:0]
	s.dirtyExec = false
}

// markDirtyExec records a command that could not be queued. Redis aborts
// EXEC with EXECABORT when any queued command had a queueing error.
func (s *connState) markDirtyExec() {
	s.dirtyExec = true
}

// String returns the string representation of the state.
func (s *connState) String() string {
	cmds := make([]string, len(s.cmds))
	for i, cmd := range s.cmds {
		cmds[i] = cmd.Name()
	}
	return fmt.Sprintf("[inMulti=%v,commands=%v,watched=%d]", s.inMulti, cmds, len(s.watched))
}
