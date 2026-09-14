// Package slowlog implements the Redis SLOWLOG command.
package slowlog

import (
	"errors"
	"sync"
	"time"

	"github.com/tsmask/redka/server/internal/redis"
)

var ErrInvalidMaxLen = errors.New("ERR invalid slowlog max length")

// Entry represents a single slow log entry.
type Entry struct {
	ID         int64
	StartTime  int64 // Unix timestamp (seconds)
	Duration   int64 // Microseconds
	Cmd        [][]byte
	Client     string // "ip:port"
	ClientName string
}

// SlowLog manages the slow command log. Thread-safe.
type SlowLog struct {
	mu        sync.RWMutex
	entries   []Entry
	maxLen    int
	nextID    int64
	threshold time.Duration
}

// New creates a slow log with given max entries and threshold.
func New(maxLen int, threshold time.Duration) *SlowLog {
	return &SlowLog{
		maxLen:    maxLen,
		threshold: threshold,
		nextID:    1,
	}
}

// Threshold returns the current slowlog threshold.
func (s *SlowLog) Threshold() time.Duration {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.threshold
}

// SetThreshold updates the slowlog threshold.
func (s *SlowLog) SetThreshold(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.threshold = d
}

// SetMaxLen updates the max entries.
func (s *SlowLog) SetMaxLen(n int) error {
	if n < 0 {
		return ErrInvalidMaxLen
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.maxLen = n
	if len(s.entries) > n {
		s.entries = s.entries[:n]
	}
	return nil
}

// MaxLen returns the current maximum number of entries.
func (s *SlowLog) MaxLen() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.maxLen
}

// Reset clears all slow log entries.
func (s *SlowLog) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = nil
}

// Len returns current number of entries.
func (s *SlowLog) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// Push adds a new entry if the duration exceeds threshold.
func (s *SlowLog) Push(d time.Duration, cmdArgs [][]byte, client, clientName string) {
	startTime := time.Now().Unix() - int64(d.Seconds())

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.threshold < 0 || d < s.threshold || s.maxLen == 0 {
		return
	}
	entry := Entry{
		ID:         s.nextID,
		StartTime:  startTime,
		Duration:   d.Microseconds(),
		Cmd:        cmdArgs,
		Client:     client,
		ClientName: clientName,
	}

	s.nextID++
	s.entries = append([]Entry{entry}, s.entries...)
	if s.maxLen < len(s.entries) {
		s.entries = s.entries[:s.maxLen]
	}
}

// Get returns up to n entries (newest first). n=-1 means all.
func (s *SlowLog) Get(n int) []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	switch {
	case n == -1 || n > len(s.entries):
		n = len(s.entries)
	case n < 0:
		n = 0
	}
	entries := make([]Entry, n)
	copy(entries, s.entries[:n])
	return entries
}

// GetFromWriter retrieves the SlowLog from connection context.
func GetFromWriter(w redis.Writer) *SlowLog {
	if v := w.Context(redis.CtxKeySlowLog); v != nil {
		if sl, ok := v.(*SlowLog); ok {
			return sl
		}
	}
	return nil
}
