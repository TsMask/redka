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
	Version = "1.5.1"
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
	cfg := DefaultConfig()
	if err := applyFile(cfg, path); err != nil {
		return nil, err
	}
	cfg.ConfigFile = path
	return cfg, nil
}

// fileConfig uses pointers so omitted fields do not overwrite values already
// supplied by defaults or the environment.
type fileConfig struct {
	Host             *string `yaml:"host"`
	Port             *int    `yaml:"port"`
	Sock             *string `yaml:"sock"`
	DBDSN            *string `yaml:"db_dsn"`
	Password         *string `yaml:"password"`
	Databases        *int    `yaml:"databases"`
	MaxClients       *int    `yaml:"max_clients"`
	SlowlogMaxLen    *int    `yaml:"slowlog_max_len"`
	SlowlogThreshold *int    `yaml:"slowlog_threshold"`
	CleanupInterval  *int    `yaml:"cleanup_interval"`
	Verbose          *bool   `yaml:"verbose"`
	LogFile          *string `yaml:"log_file"`
}

func applyFile(cfg *ServerConfig, path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read config file: %w", err)
	}

	var file fileConfig
	if err := yaml.Unmarshal(data, &file); err != nil {
		return fmt.Errorf("parse config file: %w", err)
	}
	if file.Host != nil {
		cfg.Host = *file.Host
	}
	if file.Port != nil {
		cfg.Port = *file.Port
	}
	if file.Sock != nil {
		cfg.Sock = *file.Sock
	}
	if file.DBDSN != nil {
		cfg.DBDSN = *file.DBDSN
	}
	if file.Password != nil {
		cfg.Password = *file.Password
	}
	if file.Databases != nil {
		cfg.Databases = *file.Databases
	}
	if file.MaxClients != nil {
		cfg.MaxClients = *file.MaxClients
	}
	if file.SlowlogMaxLen != nil {
		cfg.SlowlogMaxLen = *file.SlowlogMaxLen
	}
	if file.SlowlogThreshold != nil {
		cfg.SlowlogThreshold = *file.SlowlogThreshold
	}
	if file.CleanupInterval != nil {
		cfg.CleanupInterval = *file.CleanupInterval
	}
	if file.Verbose != nil {
		cfg.Verbose = *file.Verbose
	}
	if file.LogFile != nil {
		cfg.LogFile = *file.LogFile
	}
	return nil
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
