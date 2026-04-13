package redis

import (
	"context"
	"sync"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
)

// Context keys for per-connection state.
const (
	CtxKeyConfig        = "config"
	CtxKeySelectedDB    = "selected_db"
	CtxKeyRuntime       = "runtime_stats"
	CtxKeyRequest       = "request_ctx"   // Request-scoped context for timeout/cancellation
	CtxKeyAuthenticated = "authenticated" // Authentication state for the connection
)

// ConnWriter wraps a redcon.Conn to implement the Writer interface.
// It provides map-backed context storage so that multiple subsystems
// (config, state, etc.) can share the connection without type conflicts.
type ConnWriter struct {
	conn redcon.Conn
	ctx  map[string]any
	mu   sync.RWMutex // Protects concurrent access to ctx
}

// NewConnWriter creates a ConnWriter from a redcon.Conn.
// If the conn already has a map context (set during accept), it is reused.
func NewConnWriter(conn redcon.Conn) *ConnWriter {
	existing := conn.Context()
	if m, ok := existing.(map[string]any); ok {
		return &ConnWriter{conn: conn, ctx: m}
	}
	// Fresh connection, create new context map
	ctx := make(map[string]any)
	conn.SetContext(ctx)
	return &ConnWriter{conn: conn, ctx: ctx}
}

// Context returns the value for the given key from the connection context.
func (w *ConnWriter) Context(key string) any {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.ctx[key]
}

// SetContext sets the value for the given key in the connection context.
func (w *ConnWriter) SetContext(key string, v any) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.ctx[key] = v
}

// WriteAny writes any value to the client.
func (w *ConnWriter) WriteAny(v any) {
	w.conn.WriteAny(v)
}

// WriteArray writes an array header to the client.
func (w *ConnWriter) WriteArray(count int) {
	w.conn.WriteArray(count)
}

// WriteBulk writes a bulk byte slice to the client.
func (w *ConnWriter) WriteBulk(bulk []byte) {
	w.conn.WriteBulk(bulk)
}

// WriteBulkString writes a bulk string to the client.
func (w *ConnWriter) WriteBulkString(bulk string) {
	w.conn.WriteBulkString(bulk)
}

// WriteError writes an error message to the client.
func (w *ConnWriter) WriteError(msg string) {
	w.conn.WriteError(msg)
}

// WriteInt writes an integer to the client.
func (w *ConnWriter) WriteInt(num int) {
	w.conn.WriteInt(num)
}

// WriteInt64 writes an int64 to the client.
func (w *ConnWriter) WriteInt64(num int64) {
	w.conn.WriteInt64(num)
}

// WriteNull writes a null value to the client.
func (w *ConnWriter) WriteNull() {
	w.conn.WriteNull()
}

// WriteRaw writes raw bytes to the client.
func (w *ConnWriter) WriteRaw(data []byte) {
	w.conn.WriteRaw(data)
}

// WriteString writes a simple string to the client.
func (w *ConnWriter) WriteString(str string) {
	w.conn.WriteString(str)
}

// WriteUint64 writes a uint64 to the client.
func (w *ConnWriter) WriteUint64(num uint64) {
	w.conn.WriteUint64(num)
}

// --- Typed context helpers ---

// GetConfig returns the server config from the Writer context.
func GetConfig(w Writer) *config.ServerConfig {
	if v := w.Context(CtxKeyConfig); v != nil {
		if cfg, ok := v.(*config.ServerConfig); ok {
			return cfg
		}
	}
	return nil
}

// GetSelectedDB returns the selected database index from the Writer context.
func GetSelectedDB(w Writer) int {
	if v := w.Context(CtxKeySelectedDB); v != nil {
		if idx, ok := v.(int); ok {
			return idx
		}
	}
	return 0
}

// SetSelectedDB sets the selected database index in the Writer context.
func SetSelectedDB(w Writer, idx int) {
	w.SetContext(CtxKeySelectedDB, idx)
}

// GetRuntimeStats returns runtime stats from the Writer context.
func GetRuntimeStats(w Writer) *RuntimeStats {
	if v := w.Context(CtxKeyRuntime); v != nil {
		if stats, ok := v.(*RuntimeStats); ok {
			return stats
		}
	}
	return nil
}

// SetRuntimeStats stores runtime stats in the Writer context.
func SetRuntimeStats(w Writer, stats *RuntimeStats) {
	w.SetContext(CtxKeyRuntime, stats)
}

// GetRequestCtx returns the request-scoped context from the Writer context.
// Returns context.Background() if not set.
func GetRequestCtx(w Writer) context.Context {
	if v := w.Context(CtxKeyRequest); v != nil {
		if ctx, ok := v.(context.Context); ok {
			return ctx
		}
	}
	return context.Background()
}

// SetRequestCtx stores the request-scoped context in the Writer context.
func SetRequestCtx(w Writer, ctx context.Context) {
	w.SetContext(CtxKeyRequest, ctx)
}

// IsAuthenticated returns whether the client has been authenticated.
func IsAuthenticated(w Writer) bool {
	if v := w.Context(CtxKeyAuthenticated); v != nil {
		if auth, ok := v.(bool); ok {
			return auth
		}
	}
	return false
}

// SetAuthenticated marks the connection as authenticated.
func SetAuthenticated(w Writer, auth bool) {
	w.SetContext(CtxKeyAuthenticated, auth)
}
