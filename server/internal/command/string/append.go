package string

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Append appends a value to a string.
// APPEND key value
// https://redis.io/commands/append
type Append struct {
	redis.BaseCmd
	key   string
	value []byte
}

func ParseAppend(b redis.BaseCmd) (Append, error) {
	cmd := Append{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Bytes(&cmd.value),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return Append{}, err
	}
	return cmd, nil
}

func (cmd Append) Run(w redis.Writer, red redis.Redka) (any, error) {
	length, err := red.Str().Append(cmd.key, cmd.value)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(length)
	return length, nil
}
