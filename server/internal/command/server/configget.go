package server

import (
	"fmt"
	"strings"

	"github.com/tsmask/redka/server/internal/redis"
)

// Returns the effective values of configuration parameters.
// CONFIG GET parameter [parameter ...]
// https://redis.io/commands/config-get
type ConfigGet struct {
	params []string
}

func ParseConfigGet(args [][]byte) (ConfigGet, error) {
	if len(args) < 1 {
		return ConfigGet{}, redis.ErrInvalidArgNum
	}
	cmd := ConfigGet{params: make([]string, 0, len(args))}
	if len(args) == 1 {
		cmd.params = append(cmd.params, strings.ToLower(string(args[0])))
		return cmd, nil
	}

	seen := make(map[string]struct{}, len(args))
	for _, arg := range args {
		param := strings.ToLower(string(arg))
		if _, ok := seen[param]; ok {
			continue
		}
		seen[param] = struct{}{}
		cmd.params = append(cmd.params, param)
	}
	return cmd, nil
}

func (c ConfigGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	cfg := redis.GetConfig(w)
	if cfg == nil {
		w.WriteError("ERR Internal error: no config in context")
		return false, nil
	}

	// Build values in the same order as the deduplicated parameters.
	infos := make(map[string]string, len(c.params))

	for _, param := range c.params {
		switch param {
		case "requirepass":
			// Return masked password for security
			if cfg.Password != "" {
				infos[param] = "********"
			} else {
				infos[param] = ""
			}
		case "databases":
			if cfg.Databases != 0 {
				infos[param] = fmt.Sprintf("%d", cfg.Databases)
			} else {
				infos[param] = "1"
			}
		case "bind":
			if cfg.Host != "" {
				infos[param] = cfg.Host
			} else {
				infos[param] = "0.0.0.0"
			}
		case "port":
			if cfg.Port != 0 {
				infos[param] = fmt.Sprintf("%d", cfg.Port)
			} else {
				infos[param] = "6379"
			}
		default:
			// For unknown parameters, return empty string
			infos[param] = ""
		}
	}

	// Write response as array of key-value pairs
	w.WriteArray(len(c.params) * 2)
	for _, param := range c.params {
		w.WriteString(param)
		w.WriteString(infos[param])
	}

	return true, nil
}
