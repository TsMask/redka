package server

import (
	"strconv"
	"strings"

	"github.com/tsmask/redka/server/internal/redis"
	"github.com/tsmask/redka/server/internal/slowlog"
)

// SlowLogCmd is the container for SLOWLOG subcommands.
// SLOWLOG GET [count] | SLOWLOG LEN | SLOWLOG RESET
// https://redis.io/commands/slowlog-get
type SlowLogCmd struct {
	redis.BaseCmd
	subcmd string
	count  int
}

func ParseSlowLog(b redis.BaseCmd) (SlowLogCmd, error) {
	cmd := SlowLogCmd{BaseCmd: b}
	args := cmd.Args()

	// args[0] is the subcommand (command name already stripped by BaseCmd)
	if len(args) == 0 {
		// SLOWLOG with no subcommand → defaults to GET 10
		cmd.subcmd = "GET"
		cmd.count = 10
		return cmd, nil
	}

	subcmd := strings.ToUpper(string(args[0]))
	switch subcmd {
	case "GET":
		cmd.count = 10
		if len(args) > 1 {
			if c, err := strconv.Atoi(string(args[1])); err == nil {
				cmd.count = c
			}
		}
	case "LEN", "RESET":
		cmd.count = 0
	default:
		return SlowLogCmd{}, redis.ErrSyntaxError
	}
	cmd.subcmd = subcmd
	return cmd, nil
}

func (c SlowLogCmd) Run(w redis.Writer, _ redis.Redka) (any, error) {
	sl := slowlog.GetFromWriter(w)
	if sl == nil {
		w.WriteError("ERR slowlog not available")
		return nil, nil
	}

	switch c.subcmd {
	case "GET":
		entries := sl.Get(c.count)
		// Each entry: [id, timestamp, duration, cmd-array, client, client-name]
		w.WriteArray(len(entries))
		for _, e := range entries {
			w.WriteArray(6)
			w.WriteInt64(e.ID)
			w.WriteInt64(e.StartTime)
			w.WriteInt64(e.Duration)

			// Command args array
			w.WriteArray(len(e.Cmd))
			for _, arg := range e.Cmd {
				w.WriteBulkString(string(arg))
			}

			w.WriteBulkString(e.Client)
			w.WriteBulkString(e.ClientName)
		}
		return entries, nil

	case "LEN":
		w.WriteInt(sl.Len())
		return sl.Len(), nil

	case "RESET":
		sl.Reset()
		w.WriteString("OK")
		return true, nil

	default:
		w.WriteError("ERR Unknown subcommand")
		return nil, nil
	}
}
