package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/server/internal/command"
	"github.com/tsmask/redka/server/internal/redis"
)

// createHandlers returns the server command handlers.
func createHandlers(db *config.DB) redcon.HandlerFunc {
	return logging(parse(multi(handle(db))))
}

// logging logs the command processing time.
func logging(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		slog.Debug("process command",
			"client", conn.RemoteAddr(),
			"name", string(cmd.Args[0]),
			"time", time.Since(start),
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
		name := normName(cmd)
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
func handle(db *config.DB) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		state := getState(conn)
		cw := redis.NewConnWriter(conn)

		if state.inMulti {
			handleMulti(context.Background(), cw, state, db)
		} else {
			handleSingle(context.Background(), cw, state, db)
		}
		state.clear()
	}
}

// handleMulti processes a batch of commands in a transaction.
func handleMulti(ctx context.Context, w redis.Writer, state *connState, db *config.DB) {
	// Read the selected DB index from the connection context (set by SELECT).
	// Use context to carry dbIdx so it is per-request, not per-DB-instance.
	// This eliminates the data race caused by WithDB modifying shared state.
	dbIdx := redis.GetSelectedDB(w)
	err := db.Transaction(ctx, func(tx *config.Tx) error {
		for _, pcmd := range state.cmds {
			_, err := pcmd.Run(w, redis.RedkaTx(tx, dbIdx))
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
func handleSingle(ctx context.Context, w redis.Writer, state *connState, db *config.DB) {
	pcmd := state.pop()
	dbIdx := redis.GetSelectedDB(w)

	// Wrap the command execution in UpdateContext so that commands use the
	// transaction (tx) which carries the correct dbIdx from ctx via CtxDBIdx.
	// This ensures SELECT N is respected for all single commands, not just those
	// that go through d.update(fn).
	err := db.Transaction(ctx, func(tx *config.Tx) error {
		_, err := pcmd.Run(w, redis.RedkaTx(tx, dbIdx))
		return err
	})
	if err != nil {
		slog.Warn("run single command", "name", pcmd.Name(), "err", err)
	}
}
