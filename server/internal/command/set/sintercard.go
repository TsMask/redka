package set

import (
	"strconv"

	"github.com/tsmask/redka/server/internal/redis"
)

// Sintercard returns the number of members in the intersection of multiple sets.
// SINTERCARD numkeys key [key ...] [LIMIT limit]
// https://redis.io/commands/sintercard
type Sintercard struct {
	redis.BaseCmd
	keys  []string
	limit int
}

func ParseSintercard(b redis.BaseCmd) (Sintercard, error) {
	cmd := Sintercard{BaseCmd: b, limit: 0}
	args := cmd.Args()

	if len(args) < 2 {
		return Sintercard{}, redis.ErrInvalidArgNum
	}

	nKeys, err := strconv.Atoi(string(args[0]))
	if err != nil {
		return Sintercard{}, redis.ErrInvalidInt
	}

	if len(args) < nKeys+1 {
		return Sintercard{}, redis.ErrInvalidArgNum
	}

	cmd.keys = make([]string, nKeys)
	for i := 0; i < nKeys; i++ {
		cmd.keys[i] = string(args[i+1])
	}

	// Parse optional LIMIT
	remaining := args[nKeys+1:]
	for len(remaining) > 0 {
		if string(remaining[0]) == "LIMIT" && len(remaining) >= 2 {
			cmd.limit, _ = strconv.Atoi(string(remaining[1]))
			if cmd.limit > 0 {
				remaining = remaining[2:]
			} else {
				remaining = remaining[1:]
			}
		} else {
			break
		}
	}

	return cmd, nil
}

func (cmd Sintercard) Run(w redis.Writer, red redis.Redka) (any, error) {
	count, err := red.Set().InterCard(cmd.limit, cmd.keys...)
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteInt(count)
	return count, nil
}
