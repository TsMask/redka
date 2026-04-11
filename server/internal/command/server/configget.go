package server

import (
	"fmt"

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
	cmd := ConfigGet{params: make([]string, len(args))}
	for i, arg := range args {
		cmd.params[i] = string(arg)
	}
	return cmd, nil
}

func (c ConfigGet) Run(w redis.Writer, red redis.Redka) (any, error) {
	cfg := redis.GetConfig(w)
	if cfg == nil {
		w.WriteError("ERR Internal error: no config in context")
		return false, nil
	}

	// Build response map of configuration parameters
	infos := make(map[string]string)

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
	w.WriteArray(len(infos) * 2)
	for _, param := range c.params {
		w.WriteString(param)
		w.WriteString(infos[param])
	}

	return true, nil
}
