package redis

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
)

// Context keys for per-connection state.
const (
	CtxKeyConfig              = "config"
	CtxKeySelectedDB          = "selected_db"
	CtxKeyRuntime             = "runtime_stats"
	CtxKeyRequest             = "request_ctx"           // Request-scoped context for timeout/cancellation
	CtxKeyAuthenticated       = "authenticated"         // Authentication state for the connection
	CtxKeyProtover            = "protover"              // RESP protocol version (2 or 3)
	CtxKeyConnID              = "conn_id"               // Connection ID
	CtxKeyClientName          = "clientname"            // CLIENT SETNAME value
	CtxKeyClientRegistry      = "client_registry"       // Client connection registry
	CtxKeyClientPaused        = "client_paused"         // CLIENT PAUSE state
	CtxKeyClientPauseMode     = "client_pause_mode"     // CLIENT PAUSE mode (ALL/WRITE)
	CtxKeyClientReplyMode     = "client_reply_mode"     // CLIENT REPLY mode (ON/OFF/SKIP)
	CtxKeyClientSkipReply     = "client_skip_reply"     // Skip next reply flag
	CtxKeyClientTracking      = "client_tracking"       // CLIENT TRACKING state
	CtxKeyClientTrackingRedir = "client_tracking_redir" // CLIENT TRACKING redirect target
	CtxKeyClientCaching       = "client_caching"        // CLIENT CACHING hint
	CtxKeyClientNoEvict       = "client_no_evict"       // CLIENT NO-EVICT mode
	CtxKeyClientNoTouch       = "client_no_touch"       // CLIENT NO-TOUCH mode
	CtxKeyClientLibName       = "client_lib_name"       // Client library name
	CtxKeyClientLibVer        = "client_lib_ver"        // Client library version
	CtxKeyClientInfo          = "client_info"           // Custom client info map
	CtxKeySlowLog             = "slowlog"               // SlowLog getter (defined in slowlog package)
)

// ConnWriter wraps a redcon.Conn to implement the Writer interface.
// It provides map-backed context storage so that multiple subsystems
// (config, state, etc.) can share the connection without type conflicts.
type ConnWriter struct {
	conn         redcon.Conn
	ctx          map[string]any
	mu           sync.RWMutex // Protects concurrent access to ctx
	writtenBytes int64        // Track bytes written for network I/O stats
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

// ConnID returns the client connection ID.
func (w *ConnWriter) ConnID() int64 {
	w.mu.RLock()
	defer w.mu.RUnlock()
	if v, ok := w.ctx[CtxKeyConnID]; ok {
		if id, ok := v.(int64); ok {
			return id
		}
	}
	return 0
}

// Conn returns the underlying redcon.Conn.
func (w *ConnWriter) Conn() redcon.Conn {
	return w.conn
}

// WrittenBytes returns the total number of bytes written by Write methods.
// This is used for network I/O statistics.
func (w *ConnWriter) WrittenBytes() int64 {
	return atomic.LoadInt64(&w.writtenBytes)
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
	// $+len\r\n + content + \r\n
	atomic.AddInt64(&w.writtenBytes, int64(len(bulk))+9) // adds bytes to the written counter.
}

// WriteBulkString writes a bulk string to the client.
func (w *ConnWriter) WriteBulkString(bulk string) {
	w.conn.WriteBulkString(bulk)
	// $+len\r\n + content + \r\n
	atomic.AddInt64(&w.writtenBytes, int64(len(bulk))+9) // adds bytes to the written counter.
}

// WriteError writes an error message to the client.
func (w *ConnWriter) WriteError(msg string) {
	// Record error stat before writing
	if stats := GetRuntimeStats(w); stats != nil {
		stats.OnError(msg)
	}
	w.conn.WriteError(msg)
	// - + content + \r\n
	atomic.AddInt64(&w.writtenBytes, int64(len(msg))+7) // adds bytes to the written counter.
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
	atomic.AddInt64(&w.writtenBytes, int64(len(data))) // adds bytes to the written counter.
}

// WriteString writes a simple string to the client.
func (w *ConnWriter) WriteString(str string) {
	w.conn.WriteString(str)
	// + + content + \r\n
	atomic.AddInt64(&w.writtenBytes, int64(len(str))+3) // adds bytes to the written counter.
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

// GetProtover returns the RESP protocol version (2 or 3).
func GetProtover(w Writer) int {
	if v := w.Context(CtxKeyProtover); v != nil {
		if pv, ok := v.(int); ok {
			return pv
		}
	}
	return 2 // Default to RESP2
}

// SetProtover sets the RESP protocol version in the connection context.
func SetProtover(w Writer, pv int) {
	w.SetContext(CtxKeyProtover, pv)
}

// GetConnID returns the connection ID.
func GetConnID(w Writer) int64 {
	if v := w.Context(CtxKeyConnID); v != nil {
		if id, ok := v.(int64); ok {
			return id
		}
	}
	return 0
}

// SetConnID sets the connection ID in the connection context.
func SetConnID(w Writer, id int64) {
	w.SetContext(CtxKeyConnID, id)
}

// GetClientName returns the client name set via CLIENT SETNAME or HELLO SETNAME.
func GetClientName(w Writer) string {
	if v := w.Context(CtxKeyClientName); v != nil {
		if name, ok := v.(string); ok {
			return name
		}
	}
	return ""
}

// SetClientName sets the client name in the connection context.
func SetClientName(w Writer, name string) {
	w.SetContext(CtxKeyClientName, name)
}

// GetClientRegistryFromWriter returns the client registry from the Writer context.
func GetClientRegistryFromWriter(w Writer) *ClientRegistry {
	if v := w.Context(CtxKeyClientRegistry); v != nil {
		if reg, ok := v.(*ClientRegistry); ok {
			return reg
		}
	}
	return nil
}

// --- CLIENT command helpers ---

// IsClientPaused returns whether client is paused.
func IsClientPaused(w Writer) bool {
	if v := w.Context(CtxKeyClientPaused); v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// SetClientPaused sets the client pause state.
func SetClientPaused(w Writer, paused bool) {
	w.SetContext(CtxKeyClientPaused, paused)
}

// GetClientPauseMode returns the client pause mode (ALL/WRITE).
func GetClientPauseMode(w Writer) string {
	if v := w.Context(CtxKeyClientPauseMode); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "ALL"
}

// SetClientPauseMode sets the client pause mode.
func SetClientPauseMode(w Writer, mode string) {
	w.SetContext(CtxKeyClientPauseMode, mode)
}

// GetClientReplyMode returns the client reply mode (ON/OFF/SKIP).
func GetClientReplyMode(w Writer) string {
	if v := w.Context(CtxKeyClientReplyMode); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "ON"
}

// SetClientReplyMode sets the client reply mode.
func SetClientReplyMode(w Writer, mode string) {
	w.SetContext(CtxKeyClientReplyMode, mode)
}

// SkipNextReply sets flag to skip the next reply.
func SkipNextReply(w Writer) {
	w.SetContext(CtxKeyClientSkipReply, true)
}

// ShouldSkipReply checks if next reply should be skipped and clears the flag.
func ShouldSkipReply(w Writer) bool {
	if v := w.Context(CtxKeyClientSkipReply); v != nil {
		if b, ok := v.(bool); ok && b {
			w.SetContext(CtxKeyClientSkipReply, false)
			return true
		}
	}
	return false
}

// IsClientTracking returns whether client tracking is enabled.
func IsClientTracking(w Writer) bool {
	if v := w.Context(CtxKeyClientTracking); v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// SetClientTracking enables/disables client tracking.
func SetClientTracking(w Writer, enabled bool) {
	w.SetContext(CtxKeyClientTracking, enabled)
}

// GetClientTrackingRedir returns the tracking redirect target client ID.
func GetClientTrackingRedir(w Writer) int64 {
	if v := w.Context(CtxKeyClientTrackingRedir); v != nil {
		if id, ok := v.(int64); ok {
			return id
		}
	}
	return 0
}

// SetClientTrackingRedir sets the tracking redirect target client ID.
func SetClientTrackingRedir(w Writer, id int64) {
	w.SetContext(CtxKeyClientTrackingRedir, id)
}

// IsClientCaching returns the CLIENT CACHING hint.
func IsClientCaching(w Writer) bool {
	if v := w.Context(CtxKeyClientCaching); v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return true // Default to yes
}

// SetClientCaching sets the CLIENT CACHING hint.
func SetClientCaching(w Writer, yes bool) {
	w.SetContext(CtxKeyClientCaching, yes)
}

// IsClientNoEvict returns whether no-evict mode is enabled.
func IsClientNoEvict(w Writer) bool {
	if v := w.Context(CtxKeyClientNoEvict); v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// SetClientNoEvict enables/disables no-evict mode.
func SetClientNoEvict(w Writer, enabled bool) {
	w.SetContext(CtxKeyClientNoEvict, enabled)
}

// IsClientNoTouch returns whether no-touch mode is enabled.
func IsClientNoTouch(w Writer) bool {
	if v := w.Context(CtxKeyClientNoTouch); v != nil {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}

// SetClientNoTouch enables/disables no-touch mode.
func SetClientNoTouch(w Writer, enabled bool) {
	w.SetContext(CtxKeyClientNoTouch, enabled)
}

// GetClientLibName returns the client library name.
func GetClientLibName(w Writer) string {
	if v := w.Context(CtxKeyClientLibName); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// SetClientLibName sets the client library name.
func SetClientLibName(w Writer, name string) {
	w.SetContext(CtxKeyClientLibName, name)
}

// GetClientLibVer returns the client library version.
func GetClientLibVer(w Writer) string {
	if v := w.Context(CtxKeyClientLibVer); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

// SetClientLibVer sets the client library version.
func SetClientLibVer(w Writer, ver string) {
	w.SetContext(CtxKeyClientLibVer, ver)
}

// SetClientInfo sets a custom client info key-value pair.
func SetClientInfo(w Writer, key, value string) {
	m := make(map[string]string)
	if v := w.Context(CtxKeyClientInfo); v != nil {
		if existing, ok := v.(map[string]string); ok {
			for k, val := range existing {
				m[k] = val
			}
		}
	}
	m[key] = value
	w.SetContext(CtxKeyClientInfo, m)
}
