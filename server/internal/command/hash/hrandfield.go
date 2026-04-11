package hash

import (
	"strconv"
	"strings"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/server/internal/redis"
)

// Hrandfield returns one or more random fields from a hash.
// HRANDFIELD key [count [WITHVALUES]]
// https://redis.io/commands/hrandfield
type Hrandfield struct {
	redis.BaseCmd
	key        string
	count      int
	withValues bool
}

func ParseHrandfield(b redis.BaseCmd) (Hrandfield, error) {
	cmd := Hrandfield{BaseCmd: b, count: 1}
	args := cmd.Args()

	if len(args) < 1 {
		return Hrandfield{}, redis.ErrInvalidArgNum
	}
	cmd.key = string(args[0])

	// Parse optional arguments
	// Syntax: HRANDFIELD key [count [WITHVALUES]]
	// If only WITHVALUES is provided without count, it means COUNT 1
	remaining := args[1:]
	for len(remaining) > 0 {
		arg := strings.ToUpper(string(remaining[0]))
		if arg == "WITHVALUES" || arg == "WITHSCORES" {
			// WITHSCORES is accepted but ignored (for compatibility)
			cmd.withValues = true
			remaining = remaining[1:]
		} else {
			// Try to parse as count
			count, err := strconv.Atoi(string(remaining[0]))
			if err != nil {
				return Hrandfield{}, redis.ErrInvalidInt
			}
			cmd.count = count
			remaining = remaining[1:]
			// After count, check for WITHVALUES
			if len(remaining) > 0 {
				arg = strings.ToUpper(string(remaining[0]))
				if arg == "WITHVALUES" || arg == "WITHSCORES" {
					cmd.withValues = true
					remaining = remaining[1:]
				}
			}
		}
	}

	return cmd, nil
}

func (cmd Hrandfield) Run(w redis.Writer, red redis.Redka) (any, error) {
	items, err := red.Hash().RandField(cmd.key, cmd.count, cmd.withValues)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(cmd.Error(err))
		return nil, err
	}

	if cmd.withValues {
		// Return array of field-value pairs
		w.WriteArray(len(items) * 2)
		for _, item := range items {
			w.WriteBulkString(item.Field)
			w.WriteBulk(item.Value)
		}
	} else {
		// Return just field names
		w.WriteArray(len(items))
		for _, item := range items {
			w.WriteBulkString(item.Field)
		}
	}
	return items, nil
}
