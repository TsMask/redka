// Package config provides configuration management for Redka server.
package config

import (
	"log/slog"
	"os"

	"github.com/tsmask/redka"
)

// OpenDB connects to the database.
func OpenDB(cfg *ServerConfig, logger *slog.Logger) *redka.DB {
	// Connect to the database using the inferred driver.
	driverName := InferDriverName(cfg.DBDSN)
	opts := redka.Options{
		DriverName: driverName,
		Logger:     logger,
		// Using nil for pragma sets the default options.
		// We don't want any options, so pass an empty map instead.
		Pragma: map[string]string{},
	}
	db, err := redka.Open(cfg.DBDSN, &opts)
	if err != nil {
		slog.Error("data source", "error", err)
		os.Exit(1)
	}

	slog.Info("data source", "driver", driverName)

	return db
}
