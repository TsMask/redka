package zset

import (
	"github.com/tsmask/redka/redsrv/internal/parser"
	"github.com/tsmask/redka/redsrv/internal/redis"
)

// Returns members in a sorted set within a range of indexes.
// ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]
// https://redis.io/commands/zrange
type ZRange struct {
	redis.BaseCmd
	key        string
	start      float64
	stop       float64
	byScore    bool
	rev        bool
	offset     int
	count      int
	withScores bool
}

func ParseZRange(b redis.BaseCmd) (ZRange, error) {
	cmd := ZRange{BaseCmd: b}
	err := parser.New(
		parser.String(&cmd.key),
		parser.Float(&cmd.start),
		parser.Float(&cmd.stop),
		parser.Flag("byscore", &cmd.byScore),
		parser.Flag("rev", &cmd.rev),
		parser.Named("limit", parser.Int(&cmd.offset), parser.Int(&cmd.count)),
		parser.Flag("withscores", &cmd.withScores),
	).Required(3).Run(cmd.Args())
	if err != nil {
		return ZRange{}, err
	}
	return cmd, nil
}

func (cmd ZRange) Run(w redis.Writer, red redis.Redka) (any, error) {
	rang := red.ZSet().RangeWith(cmd.key)

	start := int(cmd.start)
	stop := int(cmd.stop)

	if !cmd.byScore {
		if stop == -1 {
			stop = 2147483647
		} else if stop < -1 {
			stop = -2
		}
		if start < 0 {
			start = 0
		}
		rang = rang.ByRank(start, stop)
	} else {
		rang = rang.ByScore(cmd.start, cmd.stop)
	}

	if cmd.rev {
		rang = rang.Desc()
	}

	if cmd.offset > 0 {
		rang = rang.Offset(cmd.offset)
	}
	if cmd.count > 0 {
		rang = rang.Count(cmd.count)
	}

	items, err := rang.Run()
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
