package server

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/tsmask/redka/server/internal/redis"
	"github.com/tsmask/redka/server/internal/slowlog"
)

// Updates runtime configuration parameters.
// CONFIG SET parameter value [parameter value ...]
// https://redis.io/commands/config-set
type ConfigSet struct {
	settings []configSetting
}

type configSetting struct {
	param     string
	threshold time.Duration
	maxLen    int
}

func ParseConfigSet(args [][]byte) (ConfigSet, error) {
	if len(args) == 0 || len(args)%2 != 0 {
		return ConfigSet{}, redis.ErrInvalidArgNum
	}

	cmd := ConfigSet{settings: make([]configSetting, 0, len(args)/2)}
	for i := 0; i < len(args); i += 2 {
		param := strings.ToLower(string(args[i]))
		value := string(args[i+1])
		switch param {
		case "slowlog-log-slower-than":
			micros, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return ConfigSet{}, redis.ErrInvalidInt
			}
			if micros > math.MaxInt64/1000 {
				return ConfigSet{}, redis.ErrInvalidInt
			}
			cmd.settings = append(cmd.settings, configSetting{
				param:     param,
				threshold: time.Duration(micros) * time.Microsecond,
			})
		case "slowlog-max-len":
			maxLen, err := strconv.ParseInt(value, 10, 64)
			if err != nil || maxLen < 0 || maxLen > math.MaxInt {
				return ConfigSet{}, redis.ErrInvalidInt
			}
			cmd.settings = append(cmd.settings, configSetting{
				param:  param,
				maxLen: int(maxLen),
			})
		default:
			return ConfigSet{}, fmt.Errorf("ERR Unsupported CONFIG parameter: %s", param)
		}
	}
	return cmd, nil
}

func (c ConfigSet) Run(w redis.Writer, _ redis.Redka) (any, error) {
	sl := slowlog.GetFromWriter(w)
	if sl == nil {
		w.WriteError("ERR slowlog not available")
		return false, nil
	}

	for _, setting := range c.settings {
		switch setting.param {
		case "slowlog-log-slower-than":
			sl.SetThreshold(setting.threshold)
		case "slowlog-max-len":
			if err := sl.SetMaxLen(setting.maxLen); err != nil {
				w.WriteError(err.Error())
				return false, nil
			}
		}
	}

	w.WriteString("OK")
	return true, nil
}
