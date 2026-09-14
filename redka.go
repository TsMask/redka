// Package redka provides a Redis-compatible embedded database server.
package redka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"strconv"
	"sync"
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

	stopOnce sync.Once
	stopErrs []error
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
	ready, srv := StartAsyncWithConfig(addr, dsn, cfg)
	if err := <-ready; err != nil {
		return nil, err
	}
	return srv, nil
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
	redkaServer := &Server{db: db, srv: srv}

	started := make(chan error, 1)
	go func() {
		// server.Start reports the listen result on started. It then serves
		// on the same goroutine until the listener is closed.
		_ = srv.Start(started)
	}()
	go func() {
		err := <-started
		if err != nil {
			err = fmt.Errorf("start redcon server: %w", err)
			if closeErr := redkaServer.stop(); closeErr != nil {
				err = errors.Join(err, fmt.Errorf("close server resources: %w", closeErr))
			}
		}
		ready <- err
	}()

	return ready, redkaServer
}

// Stop stops the Redka server.
func (s *Server) Stop() error {
	slog.Info("stopping redka")
	return s.stop()
}

func (s *Server) stop() error {
	s.stopOnce.Do(func() {
		if err := s.srv.Stop(); err != nil {
			s.stopErrs = append(s.stopErrs, fmt.Errorf("stop server: %w", err))
		}
		if err := s.db.Close(); err != nil {
			s.stopErrs = append(s.stopErrs, fmt.Errorf("close database: %w", err))
		}
	})

	return errors.Join(s.stopErrs...)
}

// WaitForShutdown waits for shutdown signal (Ctrl+C or SIGTERM).
// Call this after Start or StartAsync to keep the main goroutine alive.
func (s *Server) WaitForShutdown() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	s.Stop()
}
