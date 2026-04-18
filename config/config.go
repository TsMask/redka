// Package config provides configuration management for Redka server.
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

// set by the build process
var (
	Version = "1.5.0"
	Commit  = "none"
	Date    = "unknown"
)

// ServerConfig represents the server configuration.
type ServerConfig struct {
	ConfigFile string `yaml:"-"` // Path to the configuration file

	// Server network settings
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	Sock string `yaml:"sock"` // Unix socket path

	// Database settings (single DSN field)
	DBDSN string `yaml:"db_dsn"` // Database connection string

	// Authentication settings
	Password   string `yaml:"password"`    // Redka server authentication password
	Databases  int    `yaml:"databases"`   // Number of databases (default: 16)
	MaxClients int    `yaml:"max_clients"` // Max client connections (default: 10000)

	// Slow log settings
	SlowlogMaxLen    int `yaml:"slowlog_max_len"`   // Max slowlog entries (default: 128)
	SlowlogThreshold int `yaml:"slowlog_threshold"` // Threshold in ms (default: 1500)

	// Cleanup settings
	CleanupInterval int `yaml:"cleanup_interval"` // Cleanup interval in seconds (default: 10)

	// Logging settings
	Verbose bool   `yaml:"verbose"`  // Enable verbose logging
	LogFile string `yaml:"log_file"` // Log file path (optional)
}

// DefaultConfig returns a configuration with default values.
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		Host:             "0.0.0.0",
		Port:             6379,
		Sock:             "",
		DBDSN:            ":memory:", // Default to SQLite memory
		Password:         "",
		Databases:        16,    // Redis default
		MaxClients:       10000, // Redis default
		SlowlogMaxLen:    128,   // Redis default
		SlowlogThreshold: 1500,  // 1500ms
		CleanupInterval:  10,    // 10 seconds (similar to Redis expiration check)
		Verbose:          false,
		LogFile:          "",
	}
}

// Load loads configuration from a YAML file.
func Load(path string) (*ServerConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}

	config := DefaultConfig()
	if err := yaml.Unmarshal(data, config); err != nil {
		return nil, fmt.Errorf("parse config file: %w", err)
	}

	config.ConfigFile = path
	return config, nil
}

// Validate validates the configuration.
func (c *ServerConfig) Validate() error {
	if c.Port < 1 || c.Port > 65535 {
		return fmt.Errorf("invalid port: %d", c.Port)
	}

	if c.Databases < 1 || c.Databases > 256 {
		return fmt.Errorf("databases must be between 1 and 256, got: %d", c.Databases)
	}

	// Validate DSN is provided
	if c.DBDSN == "" {
		return fmt.Errorf("db_dsn is required")
	}

	return nil
}

// Network returns the network type.
func (c *ServerConfig) Network() string {
	if c.Sock != "" {
		return "unix"
	}
	return "tcp"
}

// Address returns the address or socket path.
func (c *ServerConfig) Address() string {
	if c.Sock != "" {
		return c.Sock
	}
	return net.JoinHostPort(c.Host, strconv.Itoa(c.Port))
}
