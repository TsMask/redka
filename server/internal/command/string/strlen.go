package string

import (
	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/server/internal/redis"
)

// Strlen returns the length of a string value.
// STRLEN key
// https://redis.io/commands/strlen
type Strlen struct {
	redis.BaseCmd
	key string
}

func ParseStrlen(b redis.BaseCmd) (Strlen, error) {
	cmd := Strlen{BaseCmd: b}
	if len(cmd.Args()) != 1 {
		return Strlen{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	return cmd, nil
}

func (cmd Strlen) Run(w redis.Writer, red redis.Redka) (any, error) {
	// First check if the key exists and is a string type
	// This avoids the JOIN issue in Get that can't distinguish "not found" from "wrong type"
	k, err := red.Key().Get(cmd.key)
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return 0, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	if k.Type != core.TypeString {
		w.WriteError(cmd.Error(core.ErrKeyType))
		return nil, core.ErrKeyType
	}
	// Key exists and is a string - get the actual value to return correct length
	val, err := red.Str().Get(cmd.key)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(len(val))
	return len(val), nil
}
