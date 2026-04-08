package redis

import (
	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
)

// Context keys for per-connection state.
const (
	CtxKeyConfig     = "config"
	CtxKeySelectedDB = "selected_db"
)

// ConnWriter wraps a redcon.Conn to implement the Writer interface.
// It provides map-backed context storage so that multiple subsystems
// (config, state, etc.) can share the connection without type conflicts.
type ConnWriter struct {
	conn redcon.Conn
	ctx  map[string]any
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
	return w.ctx[key]
}

// SetContext sets the value for the given key in the connection context.
func (w *ConnWriter) SetContext(key string, v any) {
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
