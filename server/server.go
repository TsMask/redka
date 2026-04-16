// Package redsrv implements a Redis-compatible (RESP) server.
package server

import (
	"fmt"
	"log/slog"
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
}

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

	// Create slowlog with config values (threshold in µs → Duration)
	slowLog := slowlog.New(cfg.SlowlogMaxLen, time.Duration(cfg.SlowlogThreshold)*time.Microsecond)

	handler := createHandlers(db, clientRegistry, slowLog)

	accept := func(conn redcon.Conn) bool {
		// Initialize connection context map with config
		ctx := make(map[string]any)
		ctx[redis.CtxKeyConfig] = cfg
		ctx[redis.CtxKeyRuntime] = runtimeStats
		ctx[redis.CtxKeyClientRegistry] = clientRegistry
		ctx[redis.CtxKeySlowLog] = slowLog
		conn.SetContext(ctx)

		// Register the client
		clientID := clientRegistry.Add(conn, 0)
		ctx[redis.CtxKeyConnID] = clientID

		runtimeStats.OnAccept()
		slog.Info("accept connection", "client", conn.RemoteAddr(), "db", 0, "id", clientID)
		return true
	}
	closed := func(conn redcon.Conn, err error) {
		// Get client ID from context to unregister
		if ctx := conn.Context(); ctx != nil {
			if m, ok := ctx.(map[string]any); ok {
				if id, ok := m[redis.CtxKeyConnID].(int64); ok {
					clientRegistry.Remove(id)
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
	err := s.srv.ListenServeAndSignal(ready)
	if err != nil {
		return fmt.Errorf("serve: %w", err)
	}
	return nil
}

// Stop stops the server and closes the database.
func (s *Server) Stop() error {
	err := s.srv.Close()
	if err != nil {
		return fmt.Errorf("server close: %w", err)
	}
	slog.Info("redka stopped", "addr", s.addr)

	err = s.db.Close()
	if err != nil {
		return fmt.Errorf("db close: %w", err)
	}
	slog.Debug("database closed")

	return nil
}
