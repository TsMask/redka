package conn

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Changes the selected database.
// SELECT index
// https://redis.io/commands/select
type Select struct {
	redis.BaseCmd
	index int
}

func ParseSelect(b redis.BaseCmd) (Select, error) {
	cmd := Select{BaseCmd: b}
	err := parser.New(
		parser.Int(&cmd.index),
	).Required(1).Run(cmd.Args())
	if err != nil {
		return Select{}, err
	}
	return cmd, nil
}

func (c Select) Run(w redis.Writer, red redis.Redka) (any, error) {
	cfg := redis.GetConfig(w)

	// Default to 16 databases if not specified
	dbCount := 16
	if cfg != nil && cfg.Databases > 0 {
		dbCount = cfg.Databases
	}

	// Validate database index
	if c.index < 0 || c.index >= dbCount {
		w.WriteError("ERR DB index is out of range")
		return false, nil
	}

	// Store the selected database in connection context
	redis.SetSelectedDB(w, c.index)

	// Sync DB to client registry
	if registry := redis.GetClientRegistryFromWriter(w); registry != nil {
		if id := redis.GetConnID(w); id != 0 {
			registry.UpdateDB(id, c.index)
		}
	}

	w.WriteString("OK")
	return true, nil
}
