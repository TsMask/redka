package server

import "github.com/tsmask/redka/server/internal/redis"

// Synchronously saves the whole dataset to disk.
// SAVE
// https://redis.io/commands/save
type Save struct {
	redis.BaseCmd
}

func ParseSave(b redis.BaseCmd) (Save, error) {
	cmd := Save{BaseCmd: b}
	if len(cmd.Args()) != 0 {
		return Save{}, redis.ErrInvalidArgNum
	}
	return cmd, nil
}

func (cmd Save) Run(w redis.Writer, red redis.Redka) (any, error) {
	w.WriteString("OK")
	return true, nil
}
