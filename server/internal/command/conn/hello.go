package conn

import (
	"strings"

	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/server/internal/redis"
)

// Hello switches to the specified protocol version and optionally
// authenticates the connection.
// HELLO [protover [AUTH username password] [SETNAME clientname]]
// https://redis.io/commands/hello
type Hello struct {
	redis.BaseCmd
	protover   int
	username   string
	password   string
	clientname string
}

func ParseHello(b redis.BaseCmd) (Hello, error) {
	cmd := Hello{BaseCmd: b}
	args := cmd.Args()

	if len(args) > 6 {
		return Hello{}, redis.ErrSyntaxError
	}

	i := 0

	// Parse optional protover (first arg, must be integer if present)
	if i < len(args) {
		if p, err := parseIntArg(args[i]); err == nil {
			cmd.protover = p
			i++
		}
	}

	// Parse optional AUTH and SETNAME subcommands
	for i < len(args) {
		kw := strings.ToUpper(string(args[i]))
		i++

		switch kw {
		case "AUTH":
			if i+1 >= len(args) {
				return Hello{}, redis.ErrSyntaxError
			}
			cmd.username = string(args[i])
			i++
			cmd.password = string(args[i])
			i++
		case "SETNAME":
			if i >= len(args) {
				return Hello{}, redis.ErrSyntaxError
			}
			cmd.clientname = string(args[i])
			i++
		default:
			return Hello{}, redis.ErrSyntaxError
		}
	}

	return cmd, nil
}

func parseIntArg(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, redis.ErrInvalidInt
	}
	v := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, redis.ErrInvalidInt
		}
		v = v*10 + int(c-'0')
	}
	return v, nil
}

func (c Hello) Run(w redis.Writer, _ redis.Redka) (any, error) {
	// Default to RESP2 if protover not specified
	protover := c.protover
	if protover == 0 {
		protover = 2
	}

	// Validate protover
	if protover != 2 && protover != 3 {
		w.WriteError("ERR Invalid protocol version")
		return nil, nil
	}

	// Handle AUTH if provided
	if c.username != "" || c.password != "" {
		cfg := redis.GetConfig(w)
		if cfg == nil || cfg.Password == "" {
			w.WriteError("ERR Client sent AUTH, but no password is set")
			return nil, nil
		}
		// Support both "default" username and no username for non-ACL auth
		user := c.username
		if user == "" {
			user = "default"
		}
		// Non-ACL mode: accept "default" username or empty username
		if c.password != cfg.Password {
			w.WriteError("ERR invalid password")
			return nil, nil
		}
		redis.SetAuthenticated(w, true)
	}

	// Handle SETNAME (CLIENT SETNAME equivalent)
	if c.clientname != "" {
		redis.SetClientName(w, c.clientname)
	}

	// Set the protocol version in context
	redis.SetProtover(w, protover)

	// Build and return server info
	c.writeHelloResponse(w, protover)
	return nil, nil
}

func (c Hello) writeHelloResponse(w redis.Writer, protover int) {
	// Get client name if set
	clientName := redis.GetClientName(w)

	// Base pairs: server, version, proto, id, mode, role, modules (7 pairs)
	// Optional: lib-name, lib-ver (2 pairs) when clientname is set
	nBasePairs := 7
	nOptPairs := 0
	if clientName != "" {
		nOptPairs = 2
	}

	w.WriteArray((nBasePairs + nOptPairs) * 2)

	// server
	w.WriteBulkString("server")
	w.WriteBulkString("redka")
	// version
	w.WriteBulkString("version")
	w.WriteBulkString(config.Version)
	// proto
	w.WriteBulkString("proto")
	w.WriteInt(protover)
	// id
	w.WriteBulkString("id")
	w.WriteBulkString(config.Commit)
	// mode
	w.WriteBulkString("mode")
	w.WriteBulkString("standalone")
	// role
	w.WriteBulkString("role")
	w.WriteBulkString("master")
	// modules (empty array)
	w.WriteBulkString("modules")
	w.WriteArray(0)

	// lib-name and lib-ver when clientname is set
	if clientName != "" {
		w.WriteBulkString("lib-name")
		w.WriteBulkString("Redka")
		w.WriteBulkString("lib-ver")
		w.WriteBulkString(config.Version)
	}
}
