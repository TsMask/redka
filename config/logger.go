// Package config provides configuration management for Redka server.
package config

import (
	"io"
	"log/slog"
	"os"
)

// LoggerConfig setups a logger for the application.
func LoggerConfig(cfg *ServerConfig) *slog.Logger {
	logLevel := new(slog.LevelVar)

	// Set log level based on verbose flag
	if cfg.Verbose {
		logLevel.Set(slog.LevelDebug) // Debug, Info, Warn, Error
	} else {
		logLevel.Set(slog.LevelWarn) // Warn, Error only
	}

	// Create multi-writer: output to both file and stdout
	var logWriter io.Writer
	if cfg.LogFile != "" {
		// Open log file
		file, err := os.OpenFile(cfg.LogFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			slog.Error("failed to open log file, using stdout only", "error", err, "file", cfg.LogFile)
			logWriter = os.Stdout
		} else {
			// Write to both file and stdout
			logWriter = io.MultiWriter(file, os.Stdout)
		}
	} else {
		// Only stdout
		logWriter = os.Stdout
	}

	logHandler := slog.NewTextHandler(logWriter, &slog.HandlerOptions{Level: logLevel})
	logger := slog.New(logHandler)
	slog.SetDefault(logger)

	return logger
}
