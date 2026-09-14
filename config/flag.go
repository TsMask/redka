// Package config provides configuration management for Redka server.
package config

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"strconv"
)

type commandLine struct {
	configFile string
	host       string
	port       string
	sock       string
	password   string
	verbose    bool
	dbDSN      string
	set        map[string]bool
}

// DefaultConfigFile is always loaded when -c is omitted. Package and container
// installs release their configuration at this path.
const DefaultConfigFile = "/usr/local/etc/redka/redka.yaml"

// FlagConfig resolves configuration in this order:
// defaults, environment, YAML file, then explicit command-line arguments.
func FlagConfig() *ServerConfig {
	cfg := DefaultConfig()
	if err := applyEnvironment(cfg); err != nil {
		slog.Error("Invalid environment configuration", "error", err)
		os.Exit(1)
	}

	cli := &commandLine{set: make(map[string]bool)}
	registerFlags(flag.CommandLine, cli, cfg)
	flag.Parse()
	flag.Visit(func(f *flag.Flag) {
		cli.set[f.Name] = true
	})

	args := flag.Args()
	if len(args) > 1 {
		slog.Error("too many arguments; expected at most one data source")
		os.Exit(1)
	}
	if len(args) == 1 {
		cli.dbDSN = args[0]
		cli.set["db-dsn"] = true
	}

	if cli.configFile != "" {
		if err := applyFile(cfg, cli.configFile); err != nil {
			slog.Error("Failed to load config file", "error", err, "file", cli.configFile)
			os.Exit(1)
		}
		cfg.ConfigFile = cli.configFile
	}
	if err := applyCommandLine(cfg, cli); err != nil {
		slog.Error("Invalid command-line configuration", "error", err)
		os.Exit(1)
	}
	if err := cfg.Validate(); err != nil {
		slog.Error("Invalid configuration", "error", err)
		os.Exit(1)
	}
	return cfg
}

func registerFlags(fs *flag.FlagSet, cli *commandLine, cfg *ServerConfig) {
	fs.StringVar(&cli.configFile, "c", DefaultConfigFile, "configuration file path (YAML format)")
	fs.StringVar(&cli.host, "h", cfg.Host, "server host")
	fs.StringVar(&cli.port, "p", strconv.Itoa(cfg.Port), "server port")
	fs.StringVar(&cli.sock, "s", cfg.Sock, "server socket (overrides host and port)")
	fs.StringVar(&cli.password, "a", cfg.Password, "require clients to authenticate with this password")
	fs.BoolVar(&cli.verbose, "v", cfg.Verbose, "verbose logging and pprof")
}

func applyCommandLine(cfg *ServerConfig, cli *commandLine) error {
	if cli.set["h"] {
		cfg.Host = cli.host
	}
	if cli.set["p"] {
		port, err := parsePort(cli.port)
		if err != nil {
			return err
		}
		cfg.Port = port
	}
	if cli.set["s"] {
		cfg.Sock = cli.sock
	}
	if cli.set["a"] {
		cfg.Password = cli.password
	}
	if cli.set["v"] {
		cfg.Verbose = cli.verbose
	}
	if cli.set["db-dsn"] && cli.dbDSN != "" {
		cfg.DBDSN = cli.dbDSN
	}
	return nil
}

func applyEnvironment(cfg *ServerConfig) error {
	applyStringEnv("REDKA_HOST", &cfg.Host)
	applyStringEnv("REDKA_SOCK", &cfg.Sock)
	applyStringEnv("REDKA_DB_DSN", &cfg.DBDSN)
	applyStringEnv("REDKA_PASSWORD", &cfg.Password)
	applyStringEnv("REDKA_LOG_FILE", &cfg.LogFile)

	ints := []struct {
		name   string
		target *int
	}{
		{name: "REDKA_PORT", target: &cfg.Port},
		{name: "REDKA_DATABASES", target: &cfg.Databases},
		{name: "REDKA_MAX_CLIENTS", target: &cfg.MaxClients},
		{name: "REDKA_SLOWLOG_MAX_LEN", target: &cfg.SlowlogMaxLen},
		{name: "REDKA_SLOWLOG_THRESHOLD", target: &cfg.SlowlogThreshold},
		{name: "REDKA_CLEANUP_INTERVAL", target: &cfg.CleanupInterval},
	}
	for _, item := range ints {
		value := os.Getenv(item.name)
		if value == "" {
			continue
		}
		parsed, err := strconv.Atoi(value)
		if err != nil {
			return fmt.Errorf("%s: %w", item.name, err)
		}
		*item.target = parsed
	}

	if value := os.Getenv("REDKA_VERBOSE"); value != "" {
		parsed, err := strconv.ParseBool(value)
		if err != nil {
			return fmt.Errorf("REDKA_VERBOSE: %w", err)
		}
		cfg.Verbose = parsed
	}
	return nil
}

func applyStringEnv(name string, target *string) {
	if value := os.Getenv(name); value != "" {
		*target = value
	}
}

func parsePort(value string) (int, error) {
	port, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("invalid port %q: %w", value, err)
	}
	return port, nil
}
