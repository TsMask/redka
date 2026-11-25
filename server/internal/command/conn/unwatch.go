package conn

import "github.com/tsmask/redka/server/internal/redis"

// Unwatch forgets all watched keys for the current connection.
// UNWATCH
// https://redis.io/commands/unwatch
type Unwatch struct {
	redis.BaseCmd
}

func ParseUnwatch(b redis.BaseCmd) (Unwatch, error) {
	if len(b.Args()) != 0 {
		return Unwatch{}, redis.ErrInvalidArgNum
	}
	return Unwatch{BaseCmd: b}, nil
}

func (cmd Unwatch) Run(w redis.Writer, _ redis.Redka) (any, error) {
	w.WriteString("OK")
	return true, nil
}
