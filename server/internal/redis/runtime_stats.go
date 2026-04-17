package redis

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// RuntimeStats stores live server counters shared across connections.
type RuntimeStats struct {
	startedAtUnix int64
	runID         string

	totalConnections atomic.Int64
	connectedClients atomic.Int64
	totalCommands    atomic.Int64

	opsSecond   atomic.Int64
	opsInSecond atomic.Int64

	// Network I/O
	totalNetInput  atomic.Int64
	totalNetOutput atomic.Int64

	// Client tracking
	rejectedConnections atomic.Int64

	// Keyspace stats
	expiredKeys    atomic.Int64
	keyspaceHits   atomic.Int64
	keyspaceMisses atomic.Int64
	evictedKeys    atomic.Int64

	// Error stats: error type -> count
	errorStats     map[string]int64
	errorStatsMu   sync.RWMutex
}

// RuntimeSnapshot is a point-in-time view of runtime counters.
type RuntimeSnapshot struct {
	RunID                    string
	UptimeInSeconds          int64
	UptimeInDays             int64
	ConnectedClients         int64
	TotalConnectionsReceived int64
	TotalCommandsProcessed   int64
	InstantaneousOpsPerSec   int64
	TotalNetInputBytes       int64
	TotalNetOutputBytes      int64
	RejectedConnections      int64
	ExpiredKeys              int64
	KeyspaceHits             int64
	KeyspaceMisses           int64
	EvictedKeys              int64
	LruClock                 int64
	ServerTimeUsec           int64
	ErrorStats               map[string]int64
}

// NewRuntimeStats creates an initialized runtime stats container.
func NewRuntimeStats(startedAt time.Time, runID string) *RuntimeStats {
	stats := &RuntimeStats{
		startedAtUnix: startedAt.Unix(),
		runID:         runID,
		errorStats:    make(map[string]int64),
	}
	stats.opsSecond.Store(startedAt.Unix())
	return stats
}

// NewRuntimeRunID returns a random run_id string similar to Redis.
func NewRuntimeRunID() string {
	const n = 20
	buf := make([]byte, n)
	if _, err := rand.Read(buf); err != nil {
		return strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return hex.EncodeToString(buf)
}

// OnAccept records a successful client connection.
func (s *RuntimeStats) OnAccept() {
	s.totalConnections.Add(1)
	s.connectedClients.Add(1)
}

// OnClose records client disconnection.
func (s *RuntimeStats) OnClose() {
	s.connectedClients.Add(-1)
}

// OnCommand records one received command.
func (s *RuntimeStats) OnCommand(now time.Time) {
	s.totalCommands.Add(1)
	sec := now.Unix()

	for {
		cur := s.opsSecond.Load()
		if cur == sec {
			s.opsInSecond.Add(1)
			return
		}
		if sec > cur {
			if s.opsSecond.CompareAndSwap(cur, sec) {
				s.opsInSecond.Store(1)
				return
			}
			continue
		}

		// Clock skew fallback.
		s.opsInSecond.Add(1)
		return
	}
}

// Snapshot returns current runtime counters.
func (s *RuntimeStats) Snapshot(now time.Time) RuntimeSnapshot {
	uptime := now.Unix() - s.startedAtUnix
	if uptime < 0 {
		uptime = 0
	}

	ops := int64(0)
	if s.opsSecond.Load() == now.Unix() {
		ops = s.opsInSecond.Load()
	}

	return RuntimeSnapshot{
		RunID:                    s.runID,
		UptimeInSeconds:          uptime,
		UptimeInDays:             uptime / 86400,
		ConnectedClients:         s.connectedClients.Load(),
		TotalConnectionsReceived: s.totalConnections.Load(),
		TotalCommandsProcessed:   s.totalCommands.Load(),
		InstantaneousOpsPerSec:   ops,
		TotalNetInputBytes:       s.totalNetInput.Load(),
		TotalNetOutputBytes:      s.totalNetOutput.Load(),
		RejectedConnections:      s.rejectedConnections.Load(),
		ExpiredKeys:              s.expiredKeys.Load(),
		KeyspaceHits:             s.keyspaceHits.Load(),
		KeyspaceMisses:           s.keyspaceMisses.Load(),
		EvictedKeys:              s.evictedKeys.Load(),
		LruClock:                 int64(now.Unix() % (1 << 31)),
		ServerTimeUsec:           now.UnixNano() / 1000,
		ErrorStats:               s.GetErrorStats(),
	}
}

// AddNetInput adds bytes to the network input counter.
func (s *RuntimeStats) AddNetInput(bytes int64) {
	s.totalNetInput.Add(bytes)
}

// AddNetOutput adds bytes to the network output counter.
func (s *RuntimeStats) AddNetOutput(bytes int64) {
	s.totalNetOutput.Add(bytes)
}

// OnRejectedConnection increments the rejected connections counter.
func (s *RuntimeStats) OnRejectedConnection() {
	s.rejectedConnections.Add(1)
}

// OnExpiredKey increments the expired keys counter.
func (s *RuntimeStats) OnExpiredKey() {
	s.expiredKeys.Add(1)
}

// OnKeyspaceHit increments the keyspace hits counter.
func (s *RuntimeStats) OnKeyspaceHit() {
	s.keyspaceHits.Add(1)
}

// OnKeyspaceMiss increments the keyspace misses counter.
func (s *RuntimeStats) OnKeyspaceMiss() {
	s.keyspaceMisses.Add(1)
}

// OnEvictedKey increments the evicted keys counter.
func (s *RuntimeStats) OnEvictedKey() {
	s.evictedKeys.Add(1)
}

// OnError records an error occurrence for errorstats.
func (s *RuntimeStats) OnError(errMsg string) {
	if errMsg == "" {
		return
	}
	// Extract error type prefix (e.g., "ERR" from "ERR something")
	parts := strings.SplitN(errMsg, " ", 2)
	errType := parts[0]
	if errType == "" {
		return
	}

	s.errorStatsMu.Lock()
	s.errorStats[errType]++
	s.errorStatsMu.Unlock()
}

// GetErrorStats returns a copy of error statistics.
func (s *RuntimeStats) GetErrorStats() map[string]int64 {
	s.errorStatsMu.RLock()
	defer s.errorStatsMu.RUnlock()

	result := make(map[string]int64, len(s.errorStats))
	for k, v := range s.errorStats {
		result[k] = v
	}
	return result
}
