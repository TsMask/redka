// Package redka provides a Redis-compatible embedded database server.
package redka

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server"
)

// Server represents a running Redka server instance.
type Server struct {
	srv      *server.Server
	debugSrv *server.DebugServer
	db       *store.Store
}

// parseAddress parses an address string and sets host/port in config.
func parseAddress(addr string, cfg *config.ServerConfig) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		// If SplitHostPort fails, addr might be just a port or path
		if port, err := strconv.Atoi(addr); err == nil {
			cfg.Host = "0.0.0.0"
			cfg.Port = port
		}
		return
	}
	cfg.Host = host
	if port, err := strconv.Atoi(portStr); err == nil {
		cfg.Port = port
	}
}

// Start starts Redka server synchronously.
func Start(addr string, dsn string) (*Server, error) {
	return StartWithConfig(addr, dsn, nil)
}

// StartWithConfig starts Redka server synchronously with custom config.
func StartWithConfig(addr string, dsn string, cfg *config.ServerConfig) (*Server, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	parseAddress(addr, cfg)
	cfg.DBDSN = dsn

	config.LoggerConfig(cfg)

	db, err := store.Open(cfg.DBDSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}
	if _, err = db.CleanupExpiredKeys(); err != nil {
		slog.Warn("cleanup expired keys on startup", "error", err)
	}
	db.StartCleanupTicker(time.Duration(cfg.CleanupInterval) * time.Second)

	srv := server.NewWithConfig(cfg.Network(), cfg.Address(), db, cfg)

	ready := make(chan error, 1)
	go func() {
		if err := srv.Start(ready); err != nil {
			ready <- fmt.Errorf("start redcon server: %w", err)
		}
	}()
	if err := <-ready; err != nil {
		return nil, err
	}

	return &Server{db: db, srv: srv}, nil
}

// StartAsync starts Redka server in a goroutine.
// Returns a channel that receives nil on success or an error on failure.
func StartAsync(addr string, dsn string) (chan error, *Server) {
	return StartAsyncWithConfig(addr, dsn, nil)
}

// StartAsyncWithConfig starts Redka server in a goroutine with custom config.
// Returns a channel that receives nil on success or an error on failure.
func StartAsyncWithConfig(addr string, dsn string, cfg *config.ServerConfig) (chan error, *Server) {
	ready := make(chan error, 1)

	if cfg == nil {
		cfg = config.DefaultConfig()
	}
	parseAddress(addr, cfg)
	cfg.DBDSN = dsn

	config.LoggerConfig(cfg)

	db, err := store.Open(cfg.DBDSN)
	if err != nil {
		ready <- fmt.Errorf("open database: %w", err)
		return ready, nil
	}
	if _, err = db.CleanupExpiredKeys(); err != nil {
		slog.Warn("cleanup expired keys on startup", "error", err)
	}
	db.StartCleanupTicker(time.Duration(cfg.CleanupInterval) * time.Second)

	srv := server.NewWithConfig(cfg.Network(), cfg.Address(), db, cfg)

	go func() {
		if err := srv.Start(ready); err != nil {
			ready <- fmt.Errorf("start redcon server: %w", err)
		}
	}()

	return ready, &Server{db: db, srv: srv}
}

// Stop stops the Redka server.
func (s *Server) Stop() error {
	slog.Info("stopping redka")

	if err := s.srv.Stop(); err != nil {
		return fmt.Errorf("stop server: %w", err)
	}

	return nil
}

// WaitForShutdown waits for shutdown signal (Ctrl+C or SIGTERM).
// Call this after Start or StartAsync to keep the main goroutine alive.
func (s *Server) WaitForShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	s.Stop()
}
