package zset

import (
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
)

// Stores the union of multiple sorted sets in a key.
// ZUNIONSTORE dest numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>]
// https://redis.io/commands/zunionstore
type ZUnionStore struct {
	redis.BaseCmd
	dest      string
	keys      []string
	aggregate string
}

func ParseZUnionStore(b redis.BaseCmd) (ZUnionStore, error) {
	cmd := ZUnionStore{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.String(&cmd.dest),
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, "SUM(score)", "MIN(score)", "MAX(score)")),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZUnionStore{}, err
	}
	return cmd, nil
}

func (cmd ZUnionStore) Run(w redis.Writer, red redis.Redka) (any, error) {
	union := red.ZSet().UnionWith(cmd.keys...).Dest(cmd.dest)
	switch cmd.aggregate {
	case AggregateMin:
		union = union.Min()
	case AggregateMax:
		union = union.Max()
	case AggregateSum:
		union = union.Sum()
	}

	count, err := union.Store()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	w.WriteInt(count)
	return count, nil
}
