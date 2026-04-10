package set

import (
	"strconv"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/redsrv/internal/redis"
)

// Get a random member from a set.
// SRANDMEMBER key [count]
// https://redis.io/commands/srandmember
type SRandMember struct {
	redis.BaseCmd
	key   string
	count int
}

func ParseSRandMember(b redis.BaseCmd) (SRandMember, error) {
	cmd := SRandMember{BaseCmd: b}
	if len(cmd.Args()) < 1 || len(cmd.Args()) > 2 {
		return SRandMember{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(cmd.Args()[0])
	if len(cmd.Args()) == 2 {
		c, err := strconv.Atoi(string(cmd.Args()[1]))
		if err != nil {
			return SRandMember{}, redis.ErrInvalidInt
		}
		cmd.count = c
	}
	return cmd, nil
}

func (cmd SRandMember) Run(w redis.Writer, red redis.Redka) (any, error) {
	if cmd.count == 0 || cmd.count == 1 {
		elem, err := red.Set().Random(cmd.key)
		if err == core.ErrNotFound {
			w.WriteNull()
			return elem, nil
		}
		if err != nil {
			w.WriteError(cmd.Error(err))
			return nil, err
		}
		w.WriteBulk(elem)
		return elem, nil
	}

	elems, err := red.Set().RandMember(cmd.key, cmd.count)
	if err == core.ErrNotFound {
		w.WriteArray(0)
		return []any{}, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteArray(len(elems))
	for _, elem := range elems {
		w.WriteBulk(elem)
	}
	return elems, nil
}
