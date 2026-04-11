package server

import (
	"errors"
	"fmt"
	"strings"

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

// normName returns the normalized command name.
func normName(cmd redcon.Command) string {
	return strings.ToLower(string(cmd.Args[0]))
}

// ctxKeyState is the context map key for connection state.
const ctxKeyState = "state"

// getState returns the connection state.
func getState(conn redcon.Conn) *connState {
	ctx := connContext(conn)
	if v, ok := ctx[ctxKeyState]; ok {
		if s, ok := v.(*connState); ok {
			return s
		}
	}
	state := new(connState)
	ctx[ctxKeyState] = state
	return state
}

// connContext returns the context map from the connection,
// creating and setting a new one if necessary.
func connContext(conn redcon.Conn) map[string]any {
	if existing := conn.Context(); existing != nil {
		if m, ok := existing.(map[string]any); ok {
			return m
		}
	}
	ctx := make(map[string]any)
	conn.SetContext(ctx)
	return ctx
}

// connState represents the connection state.
type connState struct {
	inMulti bool
	cmds    []redis.Cmd
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
}

// String returns the string representation of the state.
func (s *connState) String() string {
	cmds := make([]string, len(s.cmds))
	for i, cmd := range s.cmds {
		cmds[i] = cmd.Name()
	}
	return fmt.Sprintf("[inMulti=%v,commands=%v]", s.inMulti, cmds)
}
