package key

import (
	"testing"

	"github.com/tsmask/redka/server/internal/redis"
)

func TestParseFlushDBScope(t *testing.T) {
	tests := []struct {
		name string
		all  bool
	}{
		{name: "flushdb", all: false},
		{name: "flushall", all: true},
		{name: "FLUSHALL", all: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cmd, err := ParseFlushDB(redis.NewBaseCmd([][]byte{[]byte(test.name)}))
			if err != nil {
				t.Fatalf("ParseFlushDB: %v", err)
			}
			if cmd.all != test.all {
				t.Fatalf("all = %v, want %v", cmd.all, test.all)
			}
		})
	}
}
