package redis

import (
	"crypto/rand"
	"encoding/hex"
	"strconv"
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
}

// RuntimeSnapshot is a point-in-time view of runtime counters.
type RuntimeSnapshot struct {
	RunID                    string
	UptimeInSeconds          int64
	ConnectedClients         int64
	TotalConnectionsReceived int64
	TotalCommandsProcessed   int64
	InstantaneousOpsPerSec   int64
}

// NewRuntimeStats creates an initialized runtime stats container.
func NewRuntimeStats(startedAt time.Time, runID string) *RuntimeStats {
	stats := &RuntimeStats{
		startedAtUnix: startedAt.Unix(),
		runID:         runID,
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
		ConnectedClients:         s.connectedClients.Load(),
		TotalConnectionsReceived: s.totalConnections.Load(),
		TotalCommandsProcessed:   s.totalCommands.Load(),
		InstantaneousOpsPerSec:   ops,
	}
}
