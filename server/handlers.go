package server

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/command"
	"github.com/tsmask/redka/server/internal/redis"
	"github.com/tsmask/redka/server/internal/slowlog"
	"gorm.io/gorm"
)

// createHandlers returns the server command handlers.
func createHandlers(db *store.Store, clientRegistry *redis.ClientRegistry, slowLog *slowlog.SlowLog) redcon.HandlerFunc {
	return timeout(
		logging(
			auth(
				parse(
					multi(
						handle(db, clientRegistry),
					),
				),
			),
			slowLog,
		),
		60*time.Second,
	)
}

// auth checks if the client is authenticated before executing commands.
// Certain commands are allowed without authentication (like AUTH, PING, ECHO, etc.)
// as defined by Redis protocol standards.
func auth(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := strings.ToLower(string(cmd.Args[0]))

		// Commands that are allowed without authentication
		// These are connection-related commands that must work before authentication
		switch name {
		case "auth", // AUTH - authenticate the connection
			"ping",      // PING - check server liveliness
			"echo",      // ECHO - echo a message (diagnostic)
			"quit",      // QUIT - close the connection
			"hello",     // HELLO - RESP3 protocol negotiation
			"cluster",   // CLUSTER - cluster commands (future)
			"readOnly",  // READONLY - for replica connections
			"readWrite", // READWRITE - to disable read-only mode
			"client":
			// CLIENT subcommands (CAPA, etc.) are allowed
			// but actual client management requires auth
			next(conn, cmd)
			return
		}

		// Check if password is configured
		cfg := redis.GetConfig(redis.NewConnWriter(conn))
		if cfg == nil || cfg.Password == "" {
			// No password configured, allow all commands
			next(conn, cmd)
			return
		}

		// Check authentication status
		cw := redis.NewConnWriter(conn)
		if !redis.IsAuthenticated(cw) {
			conn.WriteError("NOAUTH Authentication required")
			return
		}

		next(conn, cmd)
	}
}

// timeout adds request timeout control to prevent slow queries from blocking resources.
func timeout(next redcon.HandlerFunc, timeout time.Duration) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ctxMap := connContext(conn)
		ctxMap[redis.CtxKeyRequest] = ctx

		done := make(chan struct{})
		go func() {
			defer close(done)
			next(conn, cmd)
		}()

		select {
		case <-done:
			return
		case <-ctx.Done():
			slog.Warn("request timeout",
				"command", string(cmd.Args[0]),
				"client", conn.RemoteAddr(),
				"timeout", timeout,
			)
			conn.WriteError("ERR operation timeout")
		}
	}
}

// logging logs the command processing time and marks slow queries.
func logging(next redcon.HandlerFunc, sl *slowlog.SlowLog) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		elapsed := time.Since(start)

		// Push to slow log if exceeds threshold
		if sl != nil && elapsed >= sl.Threshold() {
			slog.Warn("slow query detected",
				"command", string(cmd.Args[0]),
				"client", conn.RemoteAddr(),
				"duration", elapsed,
				"threshold", sl.Threshold(),
			)

			clientName := redis.GetClientName(redis.NewConnWriter(conn))
			sl.Push(elapsed, cmd.Args, conn.RemoteAddr(), clientName)
		}

		slog.Debug("process command",
			"client", conn.RemoteAddr(),
			"name", string(cmd.Args[0]),
			"time", elapsed,
		)
	}
}

// parse parses the command arguments.
func parse(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if stats := getRuntimeStats(conn); stats != nil {
			stats.OnCommand(time.Now())
		}
		pcmd, err := command.Parse(cmd.Args)
		if err != nil {
			conn.WriteError(pcmd.Error(err))
			return
		}
		state := getState(conn)
		if err := state.push(pcmd); err != nil {
			conn.WriteError(err.Error())
			return
		}
		next(conn, cmd)
	}
}

func getRuntimeStats(conn redcon.Conn) *redis.RuntimeStats {
	ctx := connContext(conn)
	if v := ctx[redis.CtxKeyRuntime]; v != nil {
		if stats, ok := v.(*redis.RuntimeStats); ok {
			return stats
		}
	}
	return nil
}

// multi handles the MULTI, EXEC, and DISCARD commands and delegates
// the rest to the next handler either in multi or single mode.
func multi(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := strings.ToLower(string(cmd.Args[0]))
		state := getState(conn)
		if state.inMulti {
			switch name {
			case "multi":
				state.pop()
				conn.WriteError(redis.ErrNestedMulti.Error())
			case "exec":
				state.pop()
				conn.WriteArray(len(state.cmds))
				next(conn, cmd)
				state.inMulti = false
			case "discard":
				state.clear()
				conn.WriteString("OK")
				state.inMulti = false
			default:
				conn.WriteString("QUEUED")
			}
		} else {
			switch name {
			case "multi":
				state.inMulti = true
				state.pop()
				conn.WriteString("OK")
			case "exec", "discard":
				state.pop()
				conn.WriteError(redis.ErrNotInMulti.Error())
			default:
				next(conn, cmd)
			}
		}
	}
}

// handle processes the command in either multi or single mode.
func handle(db *store.Store, clientRegistry *redis.ClientRegistry) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		state := getState(conn)
		cw := redis.NewConnWriter(conn)

		reqCtx := redis.GetRequestCtx(cw)

		// Track network I/O stats
		if stats := redis.GetRuntimeStats(cw); stats != nil {
			stats.AddNetInput(int64(len(cmd.Raw)))
		}

		// Update client registry with last command
		if clientID := cw.ConnID(); clientID != 0 {
			clientRegistry.SetLastCmd(clientID, string(cmd.Args[0]))
		}

		if state.inMulti {
			handleMulti(reqCtx, cw, state, db)
		} else {
			handleSingle(reqCtx, cw, state, db)
		}

		// Track network output bytes
		if stats := redis.GetRuntimeStats(cw); stats != nil {
			stats.AddNetOutput(cw.WrittenBytes())
		}

		state.clear()
	}
}

// handleMulti processes a batch of commands in a transaction.
func handleMulti(ctx context.Context, w redis.Writer, state *connState, db *store.Store) {
	// Read the selected DB index from the connection context (set by SELECT).
	// Use context to carry dbIdx so it is per-request, not per-DB-instance.
	// This eliminates the data race caused by WithDB modifying shared state.
	dbIdx := redis.GetSelectedDB(w)
	err := db.Transaction(ctx, func(tx *gorm.DB, dialect store.Dialect) error {
		s := &store.Store{Dialect: dialect, DB: tx}
		for _, pcmd := range state.cmds {
			_, err := pcmd.Run(w, redis.NewRedka(s, dbIdx))
			if err != nil {
				slog.Warn("run multi command", "name", pcmd.Name(), "err", err)
				return err
			}
		}
		return nil
	})
	if err != nil {
		slog.Warn("run multi", "err", err)
	}
}

// handleSingle processes a single command.
func handleSingle(ctx context.Context, w redis.Writer, state *connState, db *store.Store) {
	pcmd := state.pop()
	dbIdx := redis.GetSelectedDB(w)

	// Wrap the command execution in UpdateContext so that commands use the
	// transaction (tx) which carries the correct dbIdx from ctx via CtxDBIdx.
	// This ensures SELECT N is respected for all single commands, not just those
	// that go through d.update(fn).
	err := db.Transaction(ctx, func(tx *gorm.DB, dialect store.Dialect) error {
		s := &store.Store{Dialect: dialect, DB: tx}
		_, err := pcmd.Run(w, redis.NewRedka(s, dbIdx))
		return err
	})
	if err != nil {
		slog.Warn("run single command", "name", pcmd.Name(), "err", err)
	}
}
