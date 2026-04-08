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
	config := DefaultConfig()
	var portStr string
	var configFile string
	flag.StringVar(
		&config.Host, "h",
		cmp.Or(os.Getenv("REDKA_HOST"), "localhost"),
		"server host",
	)
	flag.StringVar(
		&portStr, "p",
		cmp.Or(os.Getenv("REDKA_PORT"), "6379"),
		"server port",
	)
	flag.StringVar(
		&config.Sock, "s",
		cmp.Or(os.Getenv("REDKA_SOCK"), ""),
		"server socket (overrides host and port)",
	)
	flag.StringVar(
		&config.Password, "a",
		cmp.Or(os.Getenv("REDKA_PASSWORD"), ""),
		"require clients to authenticate with this password",
	)
	flag.StringVar(
		&configFile, "c",
		"",
		"configuration file path (YAML format)",
	)
	flag.BoolVar(&config.Verbose, "v", false, "verbose logging")
	flag.Parse()
	// Parse port
	config.Port = parsePortOrExit(portStr)

	// Load configuration from file if specified
	if configFile != "" {
		fileConfig, err := Load(configFile)
		if err != nil {
			slog.Error("Failed to load config file", "error", err)
			os.Exit(1)
		}
		// Command line arguments override file config
		if config.Host != "localhost" || os.Getenv("REDKA_HOST") != "" {
			fileConfig.Host = config.Host
		}
		if portStr != "6379" || os.Getenv("REDKA_PORT") != "" {
			fileConfig.Port = parsePortOrExit(portStr)
		}
		if config.Sock != "" || os.Getenv("REDKA_SOCK") != "" {
			fileConfig.Sock = cmp.Or(config.Sock, os.Getenv("REDKA_SOCK"))
		}
		if config.Password != "" || os.Getenv("REDKA_PASSWORD") != "" {
			fileConfig.Password = cmp.Or(config.Password, os.Getenv("REDKA_PASSWORD"))
		}
		if config.Verbose {
			fileConfig.Verbose = true
		}
		// Sync log_file from file config
		if fileConfig.LogFile != "" && config.LogFile == "" {
			config.LogFile = fileConfig.LogFile
		}
		if config.DBDSN != "" || os.Getenv("REDKA_DB_DSN") != "" {
			fileConfig.DBDSN = cmp.Or(config.DBDSN, os.Getenv("REDKA_DB_DSN"))
		}

		// Validate and use file config
		if err := fileConfig.Validate(); err != nil {
			slog.Error("Invalid configuration", "error", err)
			os.Exit(1)
		}

		config.Host = fileConfig.Host
		config.Port = fileConfig.Port
		config.Sock = fileConfig.Sock
		config.Password = fileConfig.Password
		config.Verbose = fileConfig.Verbose
		config.DBDSN = fileConfig.DBDSN
	}

	return config
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
