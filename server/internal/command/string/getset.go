package string

import (
	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/server/internal/redis"
)

// Returns the previous string value of a key after setting it to a new value.
// GETSET key value
// https://redis.io/commands/getset
type GetSet struct {
	redis.BaseCmd
	key   string
	value []byte
}

func ParseGetSet(b redis.BaseCmd) (GetSet, error) {
	cmd := GetSet{BaseCmd: b}
	if len(cmd.Args()) != 2 {
		return GetSet{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	cmd.value = cmd.Args()[1]
	return cmd, nil
}

func (cmd GetSet) Run(w redis.Writer, red redis.Redka) (any, error) {
	// Get the current value first
	oldVal, err := red.Str().Get(cmd.key)
	if err != nil && err != core.ErrNotFound {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Set the new value
	if err := red.Str().Set(cmd.key, cmd.value); err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	// Return the old value (nil if key didn't exist)
	if err == core.ErrNotFound {
		w.WriteNull()
		return core.Value(nil), nil
	}

	w.WriteBulk(oldVal)
	return oldVal, nil
}
