package zset

import (
	"github.com/tsmask/redka/redsrv/internal/parser"
	"github.com/tsmask/redka/redsrv/internal/redis"
)

// Returns the union of multiple sorted sets.
// ZUNION numkeys key [key ...] [AGGREGATE <SUM | MIN | MAX>] [WITHSCORES]
// https://redis.io/commands/zunion
type ZUnion struct {
	redis.BaseCmd
	keys       []string
	aggregate  string
	withScores bool
}

func ParseZUnion(b redis.BaseCmd) (ZUnion, error) {
	cmd := ZUnion{BaseCmd: b}
	var nKeys int
	err := parser.New(
		parser.Int(&nKeys),
		parser.StringsN(&cmd.keys, &nKeys),
		parser.Named("aggregate", parser.Enum(&cmd.aggregate, "SUM(score)", "MIN(score)", "MAX(score)")),
		parser.Flag("withscores", &cmd.withScores),
	).Required(2).Run(cmd.Args())
	if err != nil {
		return ZUnion{}, err
	}
	return cmd, nil
}

func (cmd ZUnion) Run(w redis.Writer, red redis.Redka) (any, error) {
	union := red.ZSet().UnionWith(cmd.keys...)
	switch cmd.aggregate {
	case AggregateMin:
		union = union.Min()
	case AggregateMax:
		union = union.Max()
	case AggregateSum:
		union = union.Sum()
	}

	items, err := union.Run()
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.withScores {
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulk(item.Elem)
			redis.WriteFloat(w, item.Score)
		}
	} else {
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulk(item.Elem)
		}
	}

	return items, nil
}
