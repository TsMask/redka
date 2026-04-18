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
	// Start with default config
	cfg := DefaultConfig()
	var portStr string
	var configFile string
	var hostFlag, portFlag, sockFlag, passwordFlag bool

	// Track which flags were explicitly set
	flag.StringVar(
		&cfg.Host, "h",
		cmp.Or(os.Getenv("REDKA_HOST"), "0.0.0.0"),
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

	// Track which flags were explicitly set via command line
	flag.Visit(func(f *flag.Flag) {
		switch f.Name {
		case "h":
			hostFlag = true
		case "p":
			portFlag = true
		case "s":
			sockFlag = true
		case "a":
			passwordFlag = true
		}
	})

	// Load configuration from file if specified
	if configFile != "" {
		fileConfig, err := Load(configFile)
		if err != nil {
			slog.Error("Failed to load config file", "error", err)
			os.Exit(1)
		}
		cfg = fileConfig
	}

	// Command line arguments override file config and environment variables
	if hostFlag {
		// Get the actual value from flag
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "h" {
				cfg.Host = f.Value.String()
			}
		})
	}

	if portFlag {
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "p" {
				cfg.Port = parsePortOrExit(f.Value.String())
			}
		})
	}

	if sockFlag {
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "s" {
				cfg.Sock = f.Value.String()
			}
		})
	}

	if passwordFlag {
		flag.Visit(func(f *flag.Flag) {
			if f.Name == "a" {
				cfg.Password = f.Value.String()
			}
		})
	} else if v := os.Getenv("REDKA_PASSWORD"); v != "" {
		cfg.Password = v
	}

	// Validate final config
	if err := cfg.Validate(); err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(1)
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
