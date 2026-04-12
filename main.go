package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server"
)

func init() {
	// Set up flag usage message.
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage: redka [options] <data-source>\n")
		flag.PrintDefaults()
	}
}

func main() {
	cfg := config.FlagConfig()
	config.LoggerConfig(cfg)
	// Open the database.
	db, err := store.Open(cfg.DBDSN)
	if err != nil {
		slog.Error("data source", "error", err)
		os.Exit(1)
	}

	fmt.Printf("Redka By %s\n", db.Dialect)
	fmt.Printf("Version: %s\nCommit: %s\nBuiltAt: %s\n", config.Version, config.Commit, config.Date)

	// Prepare a context to handle shutdown signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Start application and debug servers.
	ready := make(chan error, 1)
	srv := startServer(cfg, db, ready)
	debugSrv := startDebugServer(cfg, ready)
	if err := <-ready; err != nil {
		slog.Error("startup", "error", err)
		shutdown(srv, debugSrv)
		os.Exit(1)
	}

	// Wait for a shutdown signal.
	<-ctx.Done()
	shutdown(srv, debugSrv)
	slog.Info("redka stopped")
}

// startServer starts the application server.
func startServer(cfg *config.ServerConfig, db *store.Store, ready chan error) *server.Server {
	// Create the server.
	var srv *server.Server
	if cfg != nil {
		// Use already-merged config from mustReadConfig.
		srv = server.NewWithConfig(cfg.Network(), cfg.Address(), db, cfg)
	} else {
		// No authentication
		srv = server.New(cfg.Network(), cfg.Address(), db)
	}

	// Start the server.
	go func() {
		if err := srv.Start(ready); err != nil {
			ready <- fmt.Errorf("start redcon server: %w", err)
		}
	}()

	return srv
}

// startDebugServer starts the debug server.
func startDebugServer(cfg *config.ServerConfig, ready chan<- error) *server.DebugServer {
	if !cfg.Verbose {
		return nil
	}
	srv := server.NewDebug("localhost", 6060)
	go func() {
		if err := srv.Start(); err != nil {
			ready <- fmt.Errorf("start debug pprof server: %w", err)
		}
	}()
	return srv
}

// shutdown stops the main server and the debug server.
func shutdown(srv *server.Server, debugSrv *server.DebugServer) {
	slog.Info("stopping redka")

	// Stop the debug server.
	if debugSrv != nil {
		if err := debugSrv.Stop(); err != nil {
			slog.Error("stopping debug pprof server", "error", err)
		}
	}

	// Stop the main server.
	if err := srv.Stop(); err != nil {
		slog.Error("stopping redcon server", "error", err)
	}
}
