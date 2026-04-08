package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/tsmask/redka"
	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/redsrv"
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
	logger := config.LoggerConfig(cfg)

	slog.Info("starting redka", "version", config.Version, "commit", config.Commit, "built_at", config.Date)

	// Open the database.
	db := config.OpenDB(cfg, logger)

	// Prepare a context to handle shutdown signals.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
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
	slog.Info("redka started")

	// Wait for a shutdown signal.
	<-ctx.Done()
	shutdown(srv, debugSrv)
	slog.Info("redka stopped")
}

// startServer starts the application server.
func startServer(cfg *config.ServerConfig, db *redka.DB, ready chan error) *redsrv.Server {
	// Create the server.
	var srv *redsrv.Server
	if cfg != nil {
		// Use already-merged config from mustReadConfig.
		srv = redsrv.NewWithConfig(cfg.Network(), cfg.Address(), db, cfg)
	} else {
		// No authentication
		srv = redsrv.New(cfg.Network(), cfg.Address(), db)
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
func startDebugServer(cfg *config.ServerConfig, ready chan<- error) *redsrv.DebugServer {
	if !cfg.Verbose {
		return nil
	}
	srv := redsrv.NewDebug("localhost", 6060)
	go func() {
		if err := srv.Start(); err != nil {
			ready <- fmt.Errorf("start debug server: %w", err)
		}
	}()
	return srv
}

// shutdown stops the main server and the debug server.
func shutdown(srv *redsrv.Server, debugSrv *redsrv.DebugServer) {
	slog.Info("stopping redka")

	// Stop the debug server.
	if debugSrv != nil {
		if err := debugSrv.Stop(); err != nil {
			slog.Error("stopping debug server", "error", err)
		}
	}

	// Stop the main server.
	if err := srv.Stop(); err != nil {
		slog.Error("stopping redcon server", "error", err)
	}
}
