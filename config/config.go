// Package config provides configuration management for Redka server.
package config

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// set by the build process
var (
	Version = "main"
	Commit  = "none"
	Date    = "unknown"
)

// ServerConfig represents the server configuration.
type ServerConfig struct {
	// Server network settings
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
	Sock string `yaml:"sock"` // Unix socket path

	// Database settings (single DSN field)
	DBDSN string `yaml:"db_dsn"` // Database connection string

	// Authentication settings
	Password  string `yaml:"password"`  // Redka server authentication password
	Databases int    `yaml:"databases"` // Number of databases (default: 16)

	// Logging settings
	Verbose bool   `yaml:"verbose"`  // Enable verbose logging
	LogFile string `yaml:"log_file"` // Log file path (optional)
}

// DefaultConfig returns a configuration with default values.
func DefaultConfig() *ServerConfig {
	return &ServerConfig{
		Host:      "localhost",
		Port:      6379,
		Sock:      "",
		DBDSN:     "file:/redka.db?vfs=memdb", // Default to SQLite memory
		Password:  "",
		Databases: 16, // Redis default
		Verbose:   false,
		LogFile:   "",
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

	return config, nil
}

// Save saves configuration to a YAML file.
func (c *ServerConfig) Save(path string) error {
	data, err := yaml.Marshal(c)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write config file: %w", err)
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

// InferDriverName infers the database driver name from the DSN prefix.
func InferDriverName(dsn string) string {

	// PostgreSQL: postgres:// or postgresql:// or pgx://
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") || strings.HasPrefix(dsn, "pgx://") {
		return "postgres"
	}

	// MySQL: mysql:// or mariadb:// or user@tcp(host)
	if strings.HasPrefix(dsn, "mysql://") || strings.HasPrefix(dsn, "mariadb://") ||
		strings.Contains(dsn, "@tcp(") {
		return "mysql"
	}

	// SQLite: file:, sqlite:, sqlite3:, or any other format (default)
	// SQLite DSN formats:
	// - file:/path/to/db.db
	// - sqlite:/path/to/db.db
	// - sqlite3:/path/to/db.db
	// Or just a plain file path like "/path/to/db.db"
	return "sqlite"
}
