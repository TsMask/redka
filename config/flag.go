// Package config provides configuration management for Redka server.
package config

import (
	"cmp"
	"flag"
	"log/slog"
	"os"
	"strconv"
)

// FlagConfig reads the configuration from
// command line arguments and environment variables.
func FlagConfig() *ServerConfig {
	cfg := DefaultConfig()
	var portStr string
	var configFile string
	flag.StringVar(
		&cfg.Host, "h",
		cmp.Or(os.Getenv("REDKA_HOST"), "localhost"),
		"server host",
	)
	flag.StringVar(
		&portStr, "p",
		cmp.Or(os.Getenv("REDKA_PORT"), "6379"),
		"server port",
	)
	flag.StringVar(
		&cfg.Sock, "s",
		cmp.Or(os.Getenv("REDKA_SOCK"), ""),
		"server socket (overrides host and port)",
	)
	flag.StringVar(
		&cfg.Password, "a",
		cmp.Or(os.Getenv("REDKA_PASSWORD"), ""),
		"require clients to authenticate with this password",
	)
	flag.StringVar(
		&configFile, "c",
		"",
		"configuration file path (YAML format)",
	)
	flag.BoolVar(&cfg.Verbose, "v", false, "verbose logging")
	flag.Parse()
	// Parse port
	cfg.Port = parsePortOrExit(portStr)

	// Load configuration from file if specified
	if configFile != "" {
		fileConfig, err := Load(configFile)
		if err != nil {
			slog.Error("Failed to load config file", "error", err)
			os.Exit(1)
		}
		// Command line arguments override file config
		if v := os.Getenv("REDKA_HOST"); v != "" {
			fileConfig.Host = v
		}
		if v := os.Getenv("REDKA_PORT"); v != "" {
			fileConfig.Port = parsePortOrExit(v)
		}
		if v := os.Getenv("REDKA_SOCK"); v != "" {
			fileConfig.Sock = v
		}
		if v := os.Getenv("REDKA_PASSWORD"); v != "" {
			fileConfig.Password = v
		}
		if v := os.Getenv("REDKA_DB_DSN"); v != "" {
			fileConfig.DBDSN = v
		}

		// Validate and use file config
		if err := fileConfig.Validate(); err != nil {
			slog.Error("Invalid configuration", "error", err)
			os.Exit(1)
		}
		cfg = fileConfig
	}

	return cfg
}

// parsePortOrExit parses a port string to an integer.
// If the port is invalid, it exits the program with an error message.
func parsePortOrExit(v string) int {
	port, err := strconv.Atoi(v)
	if err != nil {
		slog.Error("Invalid port", "value", v, "error", err)
		os.Exit(1)
	}
	return port
}
