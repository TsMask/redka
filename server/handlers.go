package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/command"
	cmdserver "github.com/tsmask/redka/server/internal/command/server"
	"github.com/tsmask/redka/server/internal/redis"
	"github.com/tsmask/redka/server/internal/slowlog"
	"gorm.io/gorm"
)

// createHandlers returns the server command handlers.
func createHandlers(
	db *store.Store, clientRegistry *redis.ClientRegistry, slowLog *slowlog.SlowLog,
) (redcon.HandlerFunc, *watchTracker) {
	tracker := newWatchTracker()
	handler := timeout(
		logging(
			auth(
				watch(
					tracker, db,
					parse(
						multi(
							tracker, db,
							handle(db, clientRegistry, tracker),
						),
					),
				),
			),
			slowLog,
		),
		60*time.Second,
	)
	return handler, tracker
}

// auth checks if the client is authenticated before executing commands.
// Certain commands are allowed without authentication (like AUTH, PING, ECHO, etc.)
// as defined by Redis protocol standards.
func auth(next redcon.HandlerFunc) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		if allowedWithoutAuthentication(cmd.Args) {
			next(conn, cmd)
			return
		}

		ctx := connContext(conn)
		password := ""
		if v, ok := ctx.Load(redis.CtxKeyConfig); ok {
			if cfg, ok := v.(*config.ServerConfig); ok {
				password = cfg.Password
			}
		}
		if password == "" {
			next(conn, cmd)
			return
		}

		authenticated := false
		if v, ok := ctx.Load(redis.CtxKeyAuthenticated); ok {
			authenticated, _ = v.(bool)
		}
		if !authenticated {
			conn.WriteError("NOAUTH Authentication required")
			return
		}

		next(conn, cmd)
	}
}

func allowedWithoutAuthentication(args [][]byte) bool {
	if len(args) == 0 {
		return false
	}

	name := string(args[0])
	switch {
	case strings.EqualFold(name, "auth"),
		strings.EqualFold(name, "ping"),
		strings.EqualFold(name, "echo"),
		strings.EqualFold(name, "quit"),
		strings.EqualFold(name, "hello"),
		strings.EqualFold(name, "cluster"),
		strings.EqualFold(name, "readOnly"),
		strings.EqualFold(name, "readWrite"):
		return true
	case !strings.EqualFold(name, "client") || len(args) < 2:
		return false
	}

	// Only connection-local metadata commands are safe before AUTH. In
	// particular, LIST, INFO, KILL, PAUSE, and REPLY can disclose state or
	// affect another connection, so they require authentication.
	subcommand := string(args[1])
	return strings.EqualFold(subcommand, "id") ||
		strings.EqualFold(subcommand, "getname") ||
		strings.EqualFold(subcommand, "setname") ||
		strings.EqualFold(subcommand, "setinfo")
}

// timeout adds request timeout control to prevent slow queries from blocking resources.
// Sets a deadline on the request context so that downstream database operations
// (GORM/SQL) respect the timeout and are cancelled automatically. This avoids
// spawning an extra goroutine per request and prevents goroutine leaks.
func timeout(next redcon.HandlerFunc, timeout time.Duration) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()

		ctxMap := connContext(conn)
		ctxMap.Store(redis.CtxKeyRequest, ctx)

		next(conn, cmd)
	}
}

// logging logs the command processing time and marks slow queries.
func logging(next redcon.HandlerFunc, sl *slowlog.SlowLog) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		start := time.Now()
		next(conn, cmd)
		elapsed := time.Since(start)

		// Push to slow log if exceeds threshold
		threshold := time.Duration(-1)
		if sl != nil {
			threshold = sl.Threshold()
		}
		if sl != nil && threshold >= 0 && elapsed >= threshold {
			slog.Warn("slow query detected",
				"command", string(cmd.Args[0]),
				"client", conn.RemoteAddr(),
				"duration", elapsed,
				"threshold", threshold,
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
			if state := getState(conn); state.inMulti {
				state.markDirtyExec()
			}
			conn.WriteError(pcmd.Error(err))
			return
		}
		name := strings.ToLower(string(cmd.Args[0]))
		_, isUnknown := pcmd.(cmdserver.Unknown)
		// MULTI, EXEC, and DISCARD are handled by name before the command
		// queue; command.Parse returns its placeholder for them.
		if isUnknown && name != "multi" && name != "exec" && name != "discard" {
			if state := getState(conn); state.inMulti {
				state.markDirtyExec()
			}
			conn.WriteError(pcmd.Error(redis.ErrUnknownCmd))
			return
		}
		state := getState(conn)
		if err := state.push(pcmd); err != nil {
			if state.inMulti {
				state.markDirtyExec()
			}
			conn.WriteError(err.Error())
			return
		}
		next(conn, cmd)
	}
}

// watch handles WATCH and the out-of-MULTI form of UNWATCH before command
// parsing, so WATCH never enters the transaction queue. Inside MULTI, UNWATCH
// is a normal queued command and runs during EXEC.
func watch(
	tracker *watchTracker, db *store.Store, next redcon.HandlerFunc,
) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := strings.ToLower(string(cmd.Args[0]))
		switch name {
		case "watch":
			trackCommand(conn, "watch")
			if len(cmd.Args) < 2 {
				if state := getState(conn); state.inMulti {
					state.markDirtyExec()
				}
				conn.WriteError(wrongArgCount("watch"))
				return
			}
			state := getState(conn)
			if state.inMulti {
				conn.WriteError(redis.ErrWatchInsideMulti.Error())
				return
			}
			dbIdx := connDBIndex(conn)
			var newKeys []watchKey
			seen := make(map[watchKey]struct{}, len(cmd.Args)-1)
			for _, arg := range cmd.Args[1:] {
				key := watchKey{db: dbIdx, key: string(arg)}
				if _, ok := seen[key]; ok {
					continue
				}
				seen[key] = struct{}{}
				if state.hasWatch(key.db, key.key) {
					continue
				}
				newKeys = append(newKeys, key)
			}
			if len(newKeys) > 0 {
				baselines, err := watchBaselines(
					connRequestContext(conn), db, tracker, newKeys)
				if err != nil {
					conn.WriteError("ERR WATCH failed: " + err.Error())
					return
				}
				for _, key := range newKeys {
					state.addWatch(key.db, key.key, baselines[key])
					tracker.addWatcher(key.db, key.key)
				}
			}
			updateWatchCount(conn, state.watchCount())
			conn.WriteString("OK")

		case "unwatch":
			if len(cmd.Args) != 1 {
				trackCommand(conn, "unwatch")
				if state := getState(conn); state.inMulti {
					state.markDirtyExec()
				}
				conn.WriteError(wrongArgCount("unwatch"))
				return
			}
			state := getState(conn)
			if state.inMulti {
				next(conn, cmd)
				return
			}
			trackCommand(conn, "unwatch")
			state.clearWatches(tracker)
			updateWatchCount(conn, 0)
			conn.WriteString("OK")

		default:
			next(conn, cmd)
		}
	}
}

func wrongArgCount(name string) string {
	return fmt.Sprintf("ERR wrong number of arguments for '%s' command", name)
}

// connRequestContext returns the request-scoped context for the current command.
func connRequestContext(conn redcon.Conn) context.Context {
	if v, ok := connContext(conn).Load(redis.CtxKeyRequest); ok {
		if ctx, ok := v.(context.Context); ok {
			return ctx
		}
	}
	return context.Background()
}

// trackCommand records a command handled before the parse middleware, so
// WATCH and out-of-MULTI UNWATCH appear in stats and CLIENT LIST.
func trackCommand(conn redcon.Conn, name string) {
	if stats := getRuntimeStats(conn); stats != nil {
		stats.OnCommand(time.Now())
	}
	ctx := connContext(conn)
	registry, _ := ctx.Load(redis.CtxKeyClientRegistry)
	clientRegistry, _ := registry.(*redis.ClientRegistry)
	if clientRegistry == nil {
		return
	}
	id, _ := ctx.Load(redis.CtxKeyConnID)
	clientID, _ := id.(int64)
	if clientID != 0 {
		clientRegistry.SetLastCmd(clientID, name)
	}
}

func getRuntimeStats(conn redcon.Conn) *redis.RuntimeStats {
	ctx := connContext(conn)
	if v, ok := ctx.Load(redis.CtxKeyRuntime); ok {
		if stats, ok := v.(*redis.RuntimeStats); ok {
			return stats
		}
	}
	return nil
}

// multi handles the MULTI, EXEC, and DISCARD commands and delegates
// the rest to the next handler either in multi or single mode.
func multi(
	tracker *watchTracker, db *store.Store, next redcon.HandlerFunc,
) redcon.HandlerFunc {
	return func(conn redcon.Conn, cmd redcon.Command) {
		name := strings.ToLower(string(cmd.Args[0]))
		state := getState(conn)
		if state.inMulti {
			switch name {
			case "multi":
				if len(cmd.Args) != 1 {
					state.pop()
					state.markDirtyExec()
					conn.WriteError(wrongArgCount("multi"))
					return
				}
				state.pop()
				conn.WriteError(redis.ErrNestedMulti.Error())
			case "exec":
				if len(cmd.Args) != 1 {
					state.pop()
					state.clear()
					state.clearWatches(tracker)
					state.inMulti = false
					updateWatchCount(conn, 0)
					conn.WriteError(
						"EXECABORT Transaction discarded because of: " +
							"wrong number of arguments for 'exec' command")
					return
				}
				state.pop()

				if state.dirtyExec {
					state.clear()
					state.clearWatches(tracker)
					state.inMulti = false
					updateWatchCount(conn, 0)
					conn.WriteError(
						"EXECABORT Transaction discarded because of previous errors.")
					return
				}

				dirty := false
				if state.watchCount() > 0 {
					kvers, err := keyVersions(
						connRequestContext(conn), db, state.watchKeys())
					if err != nil {
						state.clear()
						state.clearWatches(tracker)
						state.inMulti = false
						updateWatchCount(conn, 0)
						conn.WriteError("ERR EXEC failed: " + err.Error())
						return
					}
					dirty = state.watchDirty(tracker, kvers)
				}
				if dirty {
					state.clearWatches(tracker)
					state.clear()
					state.inMulti = false
					updateWatchCount(conn, 0)
					conn.WriteArray(-1)
					return
				}
				next(conn, cmd)
				state.inMulti = false
				state.clearWatches(tracker)
				updateWatchCount(conn, 0)
			case "discard":
				if len(cmd.Args) != 1 {
					state.pop()
					state.markDirtyExec()
					conn.WriteError(wrongArgCount("discard"))
					return
				}
				state.clear()
				state.clearWatches(tracker)
				conn.WriteString("OK")
				state.inMulti = false
				updateWatchCount(conn, 0)
			default:
				conn.WriteString("QUEUED")
			}
		} else {
			switch name {
			case "multi":
				if len(cmd.Args) != 1 {
					state.pop()
					conn.WriteError(wrongArgCount("multi"))
					return
				}
				state.inMulti = true
				state.pop()
				conn.WriteString("OK")
			case "exec":
				if len(cmd.Args) != 1 {
					state.pop()
					conn.WriteError(
						"EXECABORT Transaction discarded because of: " +
							"wrong number of arguments for 'exec' command")
					return
				}
				state.pop()
				conn.WriteError(redis.ErrNotInMulti.Error())
			case "discard":
				if len(cmd.Args) != 1 {
					state.pop()
					conn.WriteError(wrongArgCount("discard"))
					return
				}
				state.pop()
				conn.WriteError(redis.ErrDiscardWithoutMulti.Error())
			default:
				next(conn, cmd)
			}
		}
	}
}

// handle processes the command in either multi or single mode.
func handle(
	db *store.Store, clientRegistry *redis.ClientRegistry, tracker *watchTracker,
) redcon.HandlerFunc {
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
			handleMulti(reqCtx, cw, state, db, tracker)
		} else {
			handleSingle(reqCtx, cw, state, db, tracker)
		}

		// Track network output bytes
		if stats := redis.GetRuntimeStats(cw); stats != nil {
			stats.AddNetOutput(cw.WrittenBytes())
		}

		state.clear()
	}
}

// handleMulti processes a batch of commands in a transaction.
func handleMulti(
	ctx context.Context, w redis.Writer, state *connState, db *store.Store, tracker *watchTracker,
) {
	// Read the selected DB index from the connection context (set by SELECT).
	// Use context to carry dbIdx so it is per-request, not per-DB-instance.
	// This eliminates the data race caused by WithDB modifying shared state.
	dbIdx := redis.GetSelectedDB(w)
	var responses *multiWriter
	var touches []watchTouch
	err := db.TransactionRetry(ctx, func(tx *gorm.DB, dialect store.Dialect) error {
		responses = newMultiWriter(w)
		touches = nil
		for i, pcmd := range state.cmds {
			// A savepoint keeps a failing command atomic even when its repository
			// has already written part of the operation before returning an error.
			touch, err := runMultiCommand(ctx, tx, dialect, dbIdx, i, pcmd, responses, tracker)
			if err != nil {
				if !isMultiRuntimeError(err) {
					return err
				}
				slog.Debug("run multi command", "name", pcmd.Name(), "err", err)
				continue
			}
			touches = append(touches, touch)
		}
		return nil
	})
	if err != nil {
		slog.Warn("run multi", "err", err)
		w.WriteError("ERR EXEC transaction failed")
		return
	}
	applyWatchTouches(tracker, touches)
	w.WriteArray(len(state.cmds))
	responses.flush()
}

func runMultiCommand(
	ctx context.Context, tx *gorm.DB, dialect store.Dialect, dbIdx, index int,
	pcmd redis.Cmd, w redis.Writer, tracker *watchTracker,
) (watchTouch, error) {
	needsSavepoint := multiNeedsSavepoint(pcmd.Name())
	var name string
	if needsSavepoint {
		name = "redka_multi_" + strconv.Itoa(index)
		if err := tx.Exec("SAVEPOINT " + name).Error; err != nil {
			return watchTouch{}, err
		}
	}

	s := store.FromTx(dialect, tx)
	red := redis.NewRedka(ctx, s, dbIdx)
	var zaddBefore map[string]float64
	if pcmd.Name() == "zadd" {
		zaddBefore = zaddScoresBefore(red, pcmd.Args())
	}
	var zincrbyExisted bool
	if pcmd.Name() == "zincrby" {
		zincrbyExisted = zincrbyExistedBefore(red, pcmd.Args())
	}
	result, runErr := pcmd.Run(w, red)
	if runErr != nil && needsSavepoint {
		if err := tx.Exec("ROLLBACK TO SAVEPOINT " + name).Error; err != nil {
			slog.Warn("rollback multi command",
				"name", pcmd.Name(), "runErr", runErr, "err", err)
			return watchTouch{}, err
		}
	}
	// COMMIT or the outer ROLLBACK releases all still-active savepoints. Avoid
	// one RELEASE round trip for every successful command in an EXEC queue.
	if runErr != nil {
		return watchTouch{}, runErr
	}
	if pcmd.Name() == "zadd" {
		return zaddTouchAfter(dbIdx, pcmd.Args(), zaddBefore, red), nil
	}
	if pcmd.Name() == "zincrby" {
		return zincrbyTouchAfter(dbIdx, pcmd.Args(), zincrbyExisted), nil
	}
	return watchTouchFor(dbIdx, pcmd.Name(), pcmd.Args(), result), nil
}

func multiNeedsSavepoint(name string) bool {
	switch name {
	// Server and connection reads. CONFIG currently supports GET only.
	case "command", "config", "dbsize", "echo", "info", "lolwut", "ping", "unwatch":
		return false

	// Key reads.
	case "exists", "keys", "pttl", "randomkey", "scan", "ttl", "type":
		return false

	// List reads.
	case "lindex", "llen", "lrange":
		return false

	// String reads.
	case "get", "getrange", "mget", "strlen", "substr":
		return false

	// Hash reads.
	case "hexists", "hget", "hgetall", "hkeys", "hlen", "hmget",
		"hrandfield", "hscan", "hstrlen", "hvals":
		return false

	// Set reads.
	case "scard", "sdiff", "sinter", "sintercard", "sismember",
		"smembers", "smismember", "srandmember", "sscan", "sunion":
		return false

	// Sorted-set reads.
	case "zcard", "zcount", "zinter", "zrange", "zrangebyscore",
		"zrank", "zrevrange", "zrevrangebyscore", "zrevrank",
		"zscan", "zscore", "zunion":
		return false

	// These mutating commands validate convertible arguments and the existing
	// key type before their first value write. A runtime error therefore
	// leaves nothing to roll back, while a SQL error still rolls back the
	// whole outer EXEC transaction. Keep unaudited writes protected.
	case "set", "setex", "psetex", "setnx",
		"hset", "hmset", "sadd", "zadd",
		"incr", "decr", "incrby", "decrby", "incrbyfloat":
		return false

	default:
		// Unknown and mutating commands conservatively get a savepoint.
		return true
	}
}

func isMultiRuntimeError(err error) bool {
	switch {
	case errors.Is(err, core.ErrArgument),
		errors.Is(err, core.ErrKeyType),
		errors.Is(err, core.ErrNotFound),
		errors.Is(err, core.ErrValueType),
		errors.Is(err, redis.ErrInvalidArgNum),
		errors.Is(err, redis.ErrInvalidCursor),
		errors.Is(err, redis.ErrInvalidExpireTime),
		errors.Is(err, redis.ErrInvalidFloat),
		errors.Is(err, redis.ErrInvalidInt),
		errors.Is(err, redis.ErrInvalidCount),
		errors.Is(err, redis.ErrNotFound),
		errors.Is(err, redis.ErrOutOfRange),
		errors.Is(err, redis.ErrSyntaxError),
		errors.Is(err, redis.ErrUnknownSubcmd):
		return true
	default:
		return false
	}
}

type multiContextUpdate struct {
	key string
	val any
}

// multiWriter defers RESP output until the outer EXEC transaction commits.
// That makes transient SQL failures safe to retry without exposing partial
// responses from an attempt that was rolled back.
type multiWriter struct {
	parent   redis.Writer
	data     []byte
	contexts []multiContextUpdate
}

func newMultiWriter(parent redis.Writer) *multiWriter {
	return &multiWriter{parent: parent}
}

func (w *multiWriter) Context(key string) any {
	return w.parent.Context(key)
}

func (w *multiWriter) SetContext(key string, val any) {
	w.contexts = append(w.contexts, multiContextUpdate{key: key, val: val})
}

func (w *multiWriter) WriteAny(v any) {
	w.data = redcon.AppendAny(w.data, v)
}

func (w *multiWriter) WriteArray(count int) {
	w.data = redcon.AppendArray(w.data, count)
}

func (w *multiWriter) WriteBulk(bulk []byte) {
	w.data = redcon.AppendBulk(w.data, bulk)
}

func (w *multiWriter) WriteBulkString(bulk string) {
	w.data = redcon.AppendBulkString(w.data, bulk)
}

func (w *multiWriter) WriteError(msg string) {
	w.data = redcon.AppendError(w.data, msg)
}

func (w *multiWriter) WriteInt(num int) {
	w.data = redcon.AppendInt(w.data, int64(num))
}

func (w *multiWriter) WriteInt64(num int64) {
	w.data = redcon.AppendInt(w.data, num)
}

func (w *multiWriter) WriteNull() {
	w.data = redcon.AppendNull(w.data)
}

func (w *multiWriter) WriteRaw(data []byte) {
	w.data = append(w.data, data...)
}

func (w *multiWriter) WriteString(str string) {
	w.data = redcon.AppendString(w.data, str)
}

func (w *multiWriter) WriteUint64(num uint64) {
	w.data = redcon.AppendUint(w.data, num)
}

func (w *multiWriter) flush() {
	for _, update := range w.contexts {
		w.parent.SetContext(update.key, update.val)
	}
	if len(w.data) > 0 {
		w.parent.WriteRaw(w.data)
	}
}

// handleSingle processes a single command.
// Repository methods own their transaction. An additional transaction here
// doubles the boundary and prevents write methods from using no-op fast paths.
func handleSingle(
	ctx context.Context, w redis.Writer, state *connState, db *store.Store, tracker *watchTracker,
) {
	pcmd := state.pop()
	dbIdx := redis.GetSelectedDB(w)

	s := db.WithContext(ctx)
	red := redis.NewRedka(ctx, s, dbIdx)
	var zaddBefore map[string]float64
	if pcmd.Name() == "zadd" {
		zaddBefore = zaddScoresBefore(red, pcmd.Args())
	}
	var zincrbyExisted bool
	if pcmd.Name() == "zincrby" {
		zincrbyExisted = zincrbyExistedBefore(red, pcmd.Args())
	}
	result, err := pcmd.Run(w, red)
	if err != nil {
		slog.Warn("run single command", "name", pcmd.Name(), "err", err)
		return
	}
	if pcmd.Name() == "zadd" {
		applyWatchTouch(tracker, zaddTouchAfter(dbIdx, pcmd.Args(), zaddBefore, red))
		return
	}
	if pcmd.Name() == "zincrby" {
		applyWatchTouch(tracker, zincrbyTouchAfter(dbIdx, pcmd.Args(), zincrbyExisted))
		return
	}
	applyWatchTouch(tracker, watchTouchFor(dbIdx, pcmd.Name(), pcmd.Args(), result))
}
