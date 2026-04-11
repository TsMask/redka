package string

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Setrange overwrites the part of the string starting at offset with value.
// SETRANGE key offset value
// https://redis.io/commands/setrange
type Setrange struct {
	redis.BaseCmd
	key    string
	offset int
	value  []byte
}

func ParseSetrange(b redis.BaseCmd) (Setrange, error) {
	cmd := Setrange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.offset),
		parser.Bytes(&cmd.value),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return Setrange{}, err
	}
	if cmd.offset < 0 {
		return Setrange{}, redis.ErrOutOfRange
	}
	return cmd, nil
}

func (cmd Setrange) Run(w redis.Writer, red redis.Redka) (any, error) {
	length, err := red.Str().SetRange(cmd.key, cmd.offset, cmd.value)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(length)
	return length, nil
}

// Getrange returns a substring of the string value.
// GETRANGE key start end
// https://redis.io/commands/getrange
type Getrange struct {
	redis.BaseCmd
	key  string
	start int
	end   int
}

func ParseGetrange(b redis.BaseCmd) (Getrange, error) {
	cmd := Getrange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Int(&cmd.start),
		parser.Int(&cmd.end),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return Getrange{}, err
	}
	return cmd, nil
}

func (cmd Getrange) Run(w redis.Writer, red redis.Redka) (any, error) {
	val, err := red.Str().GetRange(cmd.key, cmd.start, cmd.end)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteBulk(val)
	return val, nil
}
