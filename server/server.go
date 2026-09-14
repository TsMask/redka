// Package redsrv implements a Redis-compatible (RESP) server.
package server

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/tidwall/redcon"
	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/redis"
	"github.com/tsmask/redka/server/internal/slowlog"
)

// A Redis-compatible Redka server that uses the RESP protocol.
// Works with a Redka database instance.
//
// To start the server, call [Server.Start] method and wait
// for the ready channel to receive a nil value (success) or an error.
//
// To stop the server, call [Server.Stop] method.
type Server struct {
	net  string
	addr string
	srv  *redcon.Server
	db   *store.Store

	mu        sync.Mutex
	state     serverState
	serveDone chan struct{}
}

type serverState int

const (
	serverIdle serverState = iota
	serverListening
	serverStopped
)

// New creates a new Redka server with the given
// network, address and database. Does not start the server.
func New(net string, addr string, db *store.Store) *Server {
	return NewWithConfig(net, addr, db, nil)
}

// NewWithConfig creates a new Redka server with custom configuration.
func NewWithConfig(net string, addr string, db *store.Store, cfg *config.ServerConfig) *Server {
	runtimeStats := redis.NewRuntimeStats(time.Now(), redis.NewRuntimeRunID())
	clientRegistry := redis.InitClientRegistry()

	// Use provided config or default
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	// Create slowlog with config values (threshold in ms → Duration)
	slowLog := slowlog.New(cfg.SlowlogMaxLen, time.Duration(cfg.SlowlogThreshold)*time.Millisecond)

	handler := createHandlers(db, clientRegistry, slowLog)

	accept := func(conn redcon.Conn) bool {
		// Check max clients limit
		if cfg.MaxClients > 0 && clientRegistry.Count() >= cfg.MaxClients {
			runtimeStats.OnRejectedConnection()
			slog.Warn("rejected connection: max clients reached", "client", conn.RemoteAddr(), "max", cfg.MaxClients)
			return false
		}

		// Initialize connection context using sync.Map for lock-free reads
		var ctx sync.Map
		ctx.Store(redis.CtxKeyConfig, cfg)
		ctx.Store(redis.CtxKeyRuntime, runtimeStats)
		ctx.Store(redis.CtxKeyClientRegistry, clientRegistry)
		ctx.Store(redis.CtxKeySlowLog, slowLog)
		conn.SetContext(&ctx)

		// Register the client
		clientID := clientRegistry.Add(conn, 0)
		ctx.Store(redis.CtxKeyConnID, clientID)

		runtimeStats.OnAccept()
		slog.Info("accept connection", "client", conn.RemoteAddr(), "db", 0, "id", clientID)
		return true
	}
	closed := func(conn redcon.Conn, err error) {
		// Get client ID from context to unregister
		if raw := conn.Context(); raw != nil {
			if m, ok := raw.(*sync.Map); ok {
				if v, ok := m.Load(redis.CtxKeyConnID); ok {
					if id, ok := v.(int64); ok {
						clientRegistry.Remove(id)
					}
				}
			}
		}
		runtimeStats.OnClose()
		if err != nil {
			slog.Debug("close connection", "client", conn.RemoteAddr(), "error", err)
		} else {
			slog.Debug("close connection", "client", conn.RemoteAddr())
		}
	}
	return &Server{
		net:  net,
		addr: addr,
		srv:  redcon.NewServerNetwork(net, addr, handler, accept, closed),
		db:   db,
	}
}

// Start starts the server.
// If ready chan is not nil, sends a nil value when the server
// is ready to accept connections, or an error if it fails to start.
func (s *Server) Start(ready chan error) error {
	slog.Info("redka started", "addr", s.addr)

	s.mu.Lock()
	if s.state != serverIdle {
		err := errors.New("server is not idle")
		if s.state == serverStopped {
			err = errors.New("server is stopped")
		}
		s.mu.Unlock()
		if ready != nil {
			ready <- err
		}
		return err
	}

	ln, err := net.Listen(s.net, s.addr)
	if err != nil {
		s.mu.Unlock()
		if ready != nil {
			ready <- err
		}
		return fmt.Errorf("serve: %w", err)
	}

	done := make(chan struct{})
	s.serveDone = done
	s.state = serverListening
	s.mu.Unlock()

	if ready != nil {
		ready <- nil
	}

	serveErr := s.srv.Serve(ln)
	close(done)

	s.mu.Lock()
	if s.state == serverListening {
		s.state = serverIdle
	}
	s.mu.Unlock()

	if serveErr != nil {
		return fmt.Errorf("serve: %w", serveErr)
	}
	return nil
}

// Stop stops the listener and waits for the serve loop to leave the database.
// The caller that opened the database remains responsible for closing it.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.state == serverStopped {
		s.mu.Unlock()
		return nil
	}

	wasListening := s.state == serverListening
	done := s.serveDone
	s.state = serverStopped
	s.serveDone = nil
	s.mu.Unlock()

	var err error
	if wasListening {
		err = s.srv.Close()
		if done != nil {
			<-done
		}
	}

	slog.Info("redka stopped", "addr", s.addr)
	if err != nil {
		return fmt.Errorf("server close: %w", err)
	}
	return nil
}
