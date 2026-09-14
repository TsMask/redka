package key

import (
	"strings"

	"github.com/tsmask/redka/server/internal/redis"
)

// Remove all keys from the current or all logical databases.
// FLUSHDB
// FLUSHALL
// https://redis.io/commands/flushdb
type FlushDB struct {
	redis.BaseCmd
	all bool
}

func ParseFlushDB(b redis.BaseCmd) (FlushDB, error) {
	cmd := FlushDB{
		BaseCmd: b,
		all:     strings.EqualFold(b.Name(), "flushall"),
	}
	if len(cmd.Args()) != 0 {
		return FlushDB{}, redis.ErrSyntaxError
	}
	return cmd, nil
}

func (cmd FlushDB) Run(w redis.Writer, red redis.Redka) (any, error) {
	var err error
	if cmd.all {
		err = red.Key().DeleteAll()
	} else {
		err = red.Key().DeleteAllInDB()
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}
	w.WriteString("OK")
	return true, nil
}
