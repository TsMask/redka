package server

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/redsrv/internal/parser"
	"github.com/tsmask/redka/redsrv/internal/redis"
)

var infoStartTime = time.Now()

// Returns information and statistics about the server.
// INFO [section [section ...]]
// https://redis.io/commands/info
type Info struct {
	redis.BaseCmd
	sections []string
}

func ParseInfo(b redis.BaseCmd) (Info, error) {
	cmd := Info{BaseCmd: b}
	err := parser.New(
		parser.Strings(&cmd.sections),
	).Required(0).Run(cmd.Args())
	if err != nil {
		return Info{}, err
	}
	return cmd, nil
}

func (c Info) Run(w redis.Writer, red redis.Redka) (any, error) {
	sections := normalizeInfoSections(c.sections)
	text := c.buildInfo(sections, w, red)
	w.WriteBulkString(text)
	return text, nil
}

func (c Info) buildInfo(sections []string, w redis.Writer, red redis.Redka) string {
	snap := infoRuntimeSnapshot(w)
	seen := make(map[string]struct{}, len(sections))
	parts := make([]string, 0, len(sections))

	for _, s := range sections {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}

		switch s {
		case "server":
			parts = append(parts, c.infoServer(w, snap))
		case "clients":
			parts = append(parts, c.infoClients(snap))
		case "memory":
			parts = append(parts, c.infoMemory())
		case "persistence":
			parts = append(parts, c.infoPersistence())
		case "stats":
			parts = append(parts, c.infoStats(snap))
		case "replication":
			parts = append(parts, c.infoReplication())
		case "cpu":
			parts = append(parts, c.infoCPU())
		case "modules":
			parts = append(parts, c.infoModules())
		case "cluster":
			parts = append(parts, c.infoCluster())
		case "errorstats":
			parts = append(parts, c.infoErrorStats())
		case "keyspace":
			parts = append(parts, c.infoKeyspace(w, red))
		}
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\r\n") + "\r\n"
}

func (c Info) infoServer(w redis.Writer, snap redis.RuntimeSnapshot) string {
	cfg := redis.GetConfig(w)
	port := 6379
	mode := "standalone"
	runID := strconv.FormatInt(time.Now().UnixNano(), 16)
	up := int(time.Since(infoStartTime).Seconds())
	if cfg != nil && cfg.Port != 0 {
		port = cfg.Port
	}
	if snap.RunID != "" {
		runID = snap.RunID
	}
	if snap.UptimeInSeconds > 0 {
		up = int(snap.UptimeInSeconds)
	}
	return strings.Join([]string{
		"# Server",
		"redis_version:" + config.Version,
		"redis_mode:" + mode,
		"os:" + runtime.GOOS,
		"arch_bits:" + strconv.Itoa(strconv.IntSize),
		"process_id:" + strconv.Itoa(os.Getpid()),
		"run_id:" + runID,
		"tcp_port:" + strconv.Itoa(port),
		"uptime_in_seconds:" + strconv.Itoa(up),
		"uptime_in_days:" + strconv.Itoa(up/86400),
	}, "\r\n")
}

func (c Info) infoClients(snap redis.RuntimeSnapshot) string {
	return strings.Join([]string{
		"# Clients",
		"connected_clients:" + strconv.FormatInt(snap.ConnectedClients, 10),
		"blocked_clients:0",
	}, "\r\n")
}

func (c Info) infoMemory() string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return strings.Join([]string{
		"# Memory",
		"used_memory:" + strconv.FormatUint(ms.Alloc, 10),
		"used_memory_peak:" + strconv.FormatUint(ms.TotalAlloc, 10),
		"used_memory_rss:0",
		"maxmemory:0",
	}, "\r\n")
}

func (c Info) infoPersistence() string {
	return strings.Join([]string{
		"# Persistence",
		"loading:0",
		"rdb_changes_since_last_save:0",
		"rdb_bgsave_in_progress:0",
		"aof_enabled:0",
	}, "\r\n")
}

func (c Info) infoStats(snap redis.RuntimeSnapshot) string {
	return strings.Join([]string{
		"# Stats",
		"total_connections_received:" + strconv.FormatInt(snap.TotalConnectionsReceived, 10),
		"total_commands_processed:" + strconv.FormatInt(snap.TotalCommandsProcessed, 10),
		"instantaneous_ops_per_sec:" + strconv.FormatInt(snap.InstantaneousOpsPerSec, 10),
		"keyspace_hits:0",
		"keyspace_misses:0",
	}, "\r\n")
}

func (c Info) infoReplication() string {
	return strings.Join([]string{
		"# Replication",
		"role:master",
		"connected_slaves:0",
	}, "\r\n")
}

func (c Info) infoCPU() string {
	return strings.Join([]string{
		"# CPU",
		"used_cpu_sys:0.00",
		"used_cpu_user:0.00",
	}, "\r\n")
}

func (c Info) infoModules() string {
	return strings.Join([]string{
		"# Modules",
	}, "\r\n")
}

func (c Info) infoCluster() string {
	return strings.Join([]string{
		"# Cluster",
		"cluster_enabled:0",
	}, "\r\n")
}

func (c Info) infoErrorStats() string {
	return strings.Join([]string{
		"# Errorstats",
	}, "\r\n")
}

func (c Info) infoKeyspace(w redis.Writer, red redis.Redka) string {
	dbIdx := redis.GetSelectedDB(w)
	keyCount, err := red.Key().Len()
	if err != nil {
		keyCount = 0
	}
	return strings.Join([]string{
		"# Keyspace",
		fmt.Sprintf("db%d:keys=%d,expires=0,avg_ttl=0", dbIdx, keyCount),
	}, "\r\n")
}

func normalizeInfoSections(sections []string) []string {
	if len(sections) == 0 {
		return infoDefaultSections()
	}

	req := make([]string, 0, len(sections))
	for _, s := range sections {
		sec := strings.ToLower(strings.TrimSpace(s))
		switch sec {
		case "", "default":
			req = append(req, infoDefaultSections()...)
		case "all":
			req = append(req, infoAllSections()...)
		case "everything":
			req = append(req, infoEverythingSections()...)
		default:
			req = append(req, sec)
		}
	}

	return req
}

func infoDefaultSections() []string {
	return []string{
		"server",
		"clients",
		"memory",
		"persistence",
		"stats",
		"replication",
		"cpu",
		"modules",
		"errorstats",
		"cluster",
		"keyspace",
	}
}

func infoAllSections() []string {
	return []string{
		"server",
		"clients",
		"memory",
		"persistence",
		"stats",
		"replication",
		"cpu",
		"modules",
		"cluster",
		"keyspace",
		"errorstats",
	}
}

func infoEverythingSections() []string {
	// No module-generated sections yet, so keep this equal to all.
	return infoAllSections()
}

func infoRuntimeSnapshot(w redis.Writer) redis.RuntimeSnapshot {
	stats := redis.GetRuntimeStats(w)
	if stats == nil {
		return redis.RuntimeSnapshot{}
	}
	return stats.Snapshot(time.Now())
}
