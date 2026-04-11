package set

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Smismember returns whether each member exists in a set.
// SMISMEMBER key member [member ...]
// https://redis.io/commands/smismember
type Smismember struct {
	redis.BaseCmd
	key     string
	members []any
}

func ParseSmismember(b redis.BaseCmd) (Smismember, error) {
	cmd := Smismember{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Anys(&cmd.members),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return Smismember{}, err
	}
	return cmd, nil
}

func (cmd Smismember) Run(w redis.Writer, red redis.Redka) (any, error) {
	results, err := red.Set().ExistsMany(cmd.key, cmd.members...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteArray(len(results))
	for _, exists := range results {
		if exists {
			w.WriteInt(1)
		} else {
			w.WriteInt(0)
		}
	}
	return results, nil
}
