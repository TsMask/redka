package list

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Prepends one or more elements to a list.
// Creates the key if it doesn't exist.
// LPUSH key element [element ...]
// https://redis.io/commands/lpush
type LPush struct {
	redis.BaseCmd
	key     string
	elems   []any
}

func ParseLPush(b redis.BaseCmd) (LPush, error) {
	cmd := LPush{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.elems),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return LPush{}, err
	}
	return cmd, nil
}

func (cmd LPush) Run(w redis.Writer, red redis.Redka) (any, error) {
	var n int
	var err error
	// Push elements in reverse order so that the final order matches Redis behavior
	// (LPUSH a b c results in [c, b, a, ...])
	for i := len(cmd.elems) - 1; i >= 0; i-- {
		n, err = red.List().PushFront(cmd.key, cmd.elems[i])
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
	}
	w.WriteInt(n)
	return n, nil
}
