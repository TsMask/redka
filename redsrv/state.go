package redsrv

import (
	"fmt"
	"strings"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/redsrv/internal/redis"
)

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
func (s *connState) push(cmd redis.Cmd) {
	s.cmds = append(s.cmds, cmd)
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

// clear removes all commands from the state.
func (s *connState) clear() {
	s.cmds = []redis.Cmd{}
}

// String returns the string representation of the state.
func (s *connState) String() string {
	cmds := make([]string, len(s.cmds))
	for i, cmd := range s.cmds {
		cmds[i] = cmd.Name()
	}
	return fmt.Sprintf("[inMulti=%v,commands=%v]", s.inMulti, cmds)
}
