package redis

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/tidwall/redcon"
)

// ClientInfo contains information about a connected client.
type ClientInfo struct {
	ID        int64
	Name      string
	Addr      string
	Laddr     string
	FD        int
	Age       int64
	Idle      int64
	Flags     string
	DB        int
	Sub       int
	Psub      int
	Ssub      int
	Multi     int
	Watch     int
	Cmd       string
	User      string
	LibName   string
	LibVer    string
	Redir     int64
	Resp      int
	Rbp       int64
	Rbs       int64
	IOThread  int
	TotNetIn  int64
	TotNetOut int64
	TotCmds   int64
	Qbuf      int64
	QbufFree  int64
	ArgvMem   int64
	MultiMem  int64
	Obl       int64
	Oll       int64
	Omem      int64
	TotMem    int64
	Events    string
}

// FormatClientInfo formats a single client info as a string (CLIENT LIST format).
func FormatClientInfo(info ClientInfo) string {
	parts := []string{
		formatClientField("id", strconv.FormatInt(info.ID, 10)),
		formatClientField("addr", info.Addr),
		formatClientField("laddr", info.Laddr),
		formatClientField("fd", strconv.Itoa(info.FD)),
		formatClientField("name", info.Name),
		formatClientField("age", strconv.FormatInt(info.Age, 10)),
		formatClientField("idle", strconv.FormatInt(info.Idle, 10)),
		formatClientField("flags", info.Flags),
		formatClientField("db", strconv.Itoa(info.DB)),
		formatClientField("sub", strconv.Itoa(info.Sub)),
		formatClientField("psub", strconv.Itoa(info.Psub)),
		formatClientField("ssub", strconv.Itoa(info.Ssub)),
		formatClientField("multi", strconv.Itoa(info.Multi)),
		formatClientField("watch", strconv.Itoa(info.Watch)),
		formatClientField("cmd", info.Cmd),
		formatClientField("user", info.User),
		formatClientField("lib-name", info.LibName),
		formatClientField("lib-ver", info.LibVer),
		formatClientField("redir", strconv.FormatInt(info.Redir, 10)),
		formatClientField("resp", strconv.Itoa(info.Resp)),
		formatClientField("rbp", strconv.FormatInt(info.Rbp, 10)),
		formatClientField("rbs", strconv.FormatInt(info.Rbs, 10)),
		formatClientField("io-thread", strconv.Itoa(info.IOThread)),
		formatClientField("tot-net-in", strconv.FormatInt(info.TotNetIn, 10)),
		formatClientField("tot-net-out", strconv.FormatInt(info.TotNetOut, 10)),
		formatClientField("tot-cmds", strconv.FormatInt(info.TotCmds, 10)),
		formatClientField("qbuf", strconv.FormatInt(info.Qbuf, 10)),
		formatClientField("qbuf-free", strconv.FormatInt(info.QbufFree, 10)),
		formatClientField("argv-mem", strconv.FormatInt(info.ArgvMem, 10)),
		formatClientField("multi-mem", strconv.FormatInt(info.MultiMem, 10)),
		formatClientField("obl", strconv.FormatInt(info.Obl, 10)),
		formatClientField("oll", strconv.FormatInt(info.Oll, 10)),
		formatClientField("omem", strconv.FormatInt(info.Omem, 10)),
		formatClientField("tot-mem", strconv.FormatInt(info.TotMem, 10)),
		formatClientField("events", info.Events),
	}
	return strings.Join(parts, " ")
}

func formatClientField(k, v string) string {
	if v == "" {
		return fmt.Sprintf("%s=", k)
	}
	return fmt.Sprintf("%s=%s", k, v)
}

// ClientRegistry tracks all active client connections.
type ClientRegistry struct {
	mu      sync.RWMutex
	clients map[int]*clientEntry
	nextID  int64
}

type clientEntry struct {
	info      ClientInfo
	conn      redcon.Conn
	mu        sync.Mutex
	lastCmd   time.Time
	connected time.Time
	closed    atomic.Bool
}

// globalClientRegistry is the global registry for all client connections.
var globalClientRegistry atomic.Value // stores *ClientRegistry

// GetClientRegistry returns the global client registry.
func GetClientRegistry() *ClientRegistry {
	reg, ok := globalClientRegistry.Load().(*ClientRegistry)
	if !ok || reg == nil {
		reg = &ClientRegistry{
			clients: make(map[int]*clientEntry),
			nextID:  1,
		}
		globalClientRegistry.Store(reg)
	}
	return reg
}

// InitClientRegistry initializes the global client registry.
func InitClientRegistry() *ClientRegistry {
	reg := &ClientRegistry{
		clients: make(map[int]*clientEntry),
		nextID:  1,
	}
	globalClientRegistry.Store(reg)
	return reg
}

// Add registers a new client connection and returns its ID.
func (r *ClientRegistry) Add(conn redcon.Conn, db int) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	id := r.nextID
	r.nextID++

	addr := conn.RemoteAddr()
	fd := 0
	// Try to get fd from net.Conn
	if nc := conn.NetConn(); nc != nil {
		if tcpConn, ok := nc.(*net.TCPConn); ok {
			if f, err := tcpConn.File(); err == nil {
				fd = int(f.Fd())
				f.Close()
			}
		}
	}

	now := time.Now()
	entry := &clientEntry{
		info: ClientInfo{
			ID:     id,
			Addr:   addr,
			Laddr:  "", // Would need socket info
			FD:     fd,
			Age:    0,
			Idle:   0,
			Flags:  "N",
			DB:     db,
			Events: "rw",
		},
		conn:      conn,
		lastCmd:   now,
		connected: now,
	}
	r.clients[int(id)] = entry
	return id
}

// Remove unregisters a client connection.
func (r *ClientRegistry) Remove(id int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.clients, int(id))
}

// Update updates the client info for a given ID.
func (r *ClientRegistry) Update(id int64, fn func(*ClientInfo)) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	fn(&entry.info)
}

// UpdateDB updates the database index for a given client ID.
func (r *ClientRegistry) UpdateDB(id int64, db int) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	entry.info.DB = db
}

// Touch updates the last command time.
func (r *ClientRegistry) Touch(id int64) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return
	}
	entry.mu.Lock()
	entry.lastCmd = time.Now()
	entry.mu.Unlock()
}

// Get returns client info by ID.
func (r *ClientRegistry) Get(id int64) (ClientInfo, bool) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return ClientInfo{}, false
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	return entry.info, true
}

// List returns info for all clients.
func (r *ClientRegistry) List() []ClientInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	now := time.Now()
	infos := make([]ClientInfo, 0, len(r.clients))
	for _, entry := range r.clients {
		entry.mu.Lock()
		if entry.closed.Load() {
			entry.mu.Unlock()
			continue
		}
		info := entry.info
		// Age is connection age since registration
		info.Age = int64(now.Sub(entry.connected).Seconds())
		// Idle is time since last command
		info.Idle = int64(now.Sub(entry.lastCmd).Seconds())
		entry.mu.Unlock()
		infos = append(infos, info)
	}
	return infos
}

// Close closes a client connection by ID.
// Returns true if the client was found and closed.
func (r *ClientRegistry) Close(id int64) bool {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return false
	}
	if entry.closed.CompareAndSwap(false, true) {
		entry.conn.Close()
		return true
	}
	return false
}

// Count returns the number of active connections.
func (r *ClientRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.clients)
}

// CloseByAddr closes all connections to a specific address.
// Returns the number of connections closed.
func (r *ClientRegistry) CloseByAddr(addr string) int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	count := 0
	for _, entry := range r.clients {
		if entry.closed.Load() {
			continue
		}
		if entry.info.Addr == addr {
			entry.closed.Store(true)
			entry.conn.Close()
			count++
		}
	}
	return count
}

// SetConnInfo sets additional connection info.
func (r *ClientRegistry) SetConnInfo(id int64, name, libName, libVer string) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return
	}
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if name != "" {
		entry.info.Name = name
	}
	if libName != "" {
		entry.info.LibName = libName
	}
	if libVer != "" {
		entry.info.LibVer = libVer
	}
}

// SetLastCmd records the last command executed.
func (r *ClientRegistry) SetLastCmd(id int64, cmd string) {
	r.mu.RLock()
	entry, ok := r.clients[int(id)]
	r.mu.RUnlock()
	if !ok {
		return
	}
	entry.mu.Lock()
	entry.lastCmd = time.Now()
	entry.info.Cmd = cmd
	entry.info.TotCmds++
	entry.mu.Unlock()
}
