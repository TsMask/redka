package conn

import (
	"github.com/tsmask/redka/redsrv/internal/redis"
)

// Authenticates the client using a password.
// Implements Redis AUTH command for password authentication.
// AUTH [username] password
// https://redis.io/commands/auth
type Auth struct {
	redis.BaseCmd
	password string
}

func ParseAuth(b redis.BaseCmd) (Auth, error) {
	cmd := Auth{BaseCmd: b}
	if len(cmd.Args()) != 1 && len(cmd.Args()) != 2 {
		return Auth{}, redis.ErrInvalidArgNum
	}

	// Support both AUTH <password> and AUTH <username> <password>
	if len(cmd.Args()) == 1 {
		cmd.password = string(cmd.Args()[0])
	} else {
		// For Redis 6.0+ ACL style, but we just use the password
		cmd.password = string(cmd.Args()[1])
	}
	return cmd, nil
}

func (c Auth) Run(w redis.Writer, red redis.Redka) (any, error) {
	cfg := redis.GetConfig(w)
	if cfg == nil || cfg.Password == "" {
		w.WriteError("ERR Client sent AUTH, but no password is set")
		return false, nil
	}

	// Validate password
	if c.password != cfg.Password {
		w.WriteError("ERR invalid password")
		return false, nil
	}

	w.WriteString("OK")
	return true, nil
}
