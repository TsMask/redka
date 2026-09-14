package list

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Appends one or more elements to a list.
// Creates the key if it doesn't exist.
// RPUSH key element [element ...]
// https://redis.io/commands/rpush
type RPush struct {
	redis.BaseCmd
	key   string
	elems []any
}

func ParseRPush(b redis.BaseCmd) (RPush, error) {
	cmd := RPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.elems),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return RPush{}, err
	}
	return cmd, nil
}

func (cmd RPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	n, err := red.List().PushBackMany(cmd.key, cmd.elems...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(n)
	return n, nil
}
