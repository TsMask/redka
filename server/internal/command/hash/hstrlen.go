package hash

import (
	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Hstrlen returns the length of the value of a field in a hash.
// HSTRLEN key field
// https://redis.io/commands/hstrlen
type Hstrlen struct {
	redis.BaseCmd
	key   string
	field string
}

func ParseHstrlen(b redis.BaseCmd) (Hstrlen, error) {
	cmd := Hstrlen{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.String(&cmd.field),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return Hstrlen{}, err
	}
	return cmd, nil
}

func (cmd Hstrlen) Run(w redis.Writer, red redis.Redka) (any, error) {
	length, err := red.Hash().StrLen(cmd.key, cmd.field)
	if err == core.ErrNotFound {
		w.WriteInt(0)
		return 0, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(length)
	return length, nil
}
