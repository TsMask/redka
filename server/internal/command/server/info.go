package server

import (
	"fmt"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/process"
	"github.com/tsmask/redka/config"
	"github.com/tsmask/redka/internal/store"
	"github.com/tsmask/redka/server/internal/parser"
	"github.com/tsmask/redka/server/internal/redis"
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
			parts = append(parts, c.infoClients(w, snap))
		case "memory":
			parts = append(parts, c.infoMemory(snap))
		case "persistence":
			parts = append(parts, c.infoPersistence())
		case "stats":
			parts = append(parts, c.infoStats(snap))
		case "replication":
			parts = append(parts, c.infoReplication())
		case "cpu":
			parts = append(parts, c.infoCPU())
		case "modules":
			parts = append(parts, c.infoModules(red))
		case "cluster":
			parts = append(parts, c.infoCluster())
		case "errorstats":
			parts = append(parts, c.infoErrorStats(snap))
		case "keyspace":
			parts = append(parts, c.infoKeyspace(w, red))
		}
	}

	if len(parts) == 0 {
		return ""
	}
	return strings.Join(parts, "\r\n\r\n") + "\r\n"
}

func (c Info) infoServer(w redis.Writer, snap redis.RuntimeSnapshot) string {
	cfg := redis.GetConfig(w)
	port := 6379
	host := "0.0.0.0"
	configFile := ""
	if cfg != nil {
		if cfg.Port != 0 {
			port = cfg.Port
		}
		if cfg.Host != "" {
			host = cfg.Host
		}
		if cfg.ConfigFile != "" {
			configFile = cfg.ConfigFile
		}

	}
	mode := "standalone"
	runID := snap.RunID
	if runID == "" {
		runID = strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	up := int(snap.UptimeInSeconds)
	if up == 0 {
		up = int(time.Since(infoStartTime).Seconds())
	}
	upDays := up / 86400

	executable, _ := os.Executable()
	ioThreadsActive := "0"

	var lines []string
	lines = append(lines, "# Server")
	lines = append(lines, "redis_version:"+config.Version)
	lines = append(lines, "redis_mode:"+mode)
	lines = append(lines, "os:"+runtime.GOOS+" "+runtime.GOARCH)
	lines = append(lines, "arch_bits:"+strconv.Itoa(strconv.IntSize))
	lines = append(lines, "monotonic_clock:POSIX clock_gettime")
	lines = append(lines, "multiplexing_api:epoll")
	lines = append(lines, "atomicvar_api:atomic")
	lines = append(lines, "gcc_version:"+runtime.Version())
	lines = append(lines, "process_id:"+strconv.Itoa(os.Getpid()))
	lines = append(lines, "process_supervised:no")
	lines = append(lines, "run_id:"+runID)
	lines = append(lines, "tcp_port:"+strconv.Itoa(port))
	lines = append(lines, "server_time_usec:"+strconv.FormatInt(snap.ServerTimeUsec, 10))
	lines = append(lines, "uptime_in_seconds:"+strconv.Itoa(up))
	lines = append(lines, "uptime_in_days:"+strconv.Itoa(upDays))
	lines = append(lines, "hz:1")
	lines = append(lines, "configured_hz:1")
	lines = append(lines, "lru_clock:"+strconv.FormatInt(snap.LruClock, 10))
	lines = append(lines, "executable:"+executable)
	lines = append(lines, "config_file:"+configFile)
	lines = append(lines, "io_threads_active:"+ioThreadsActive)
	lines = append(lines, "listener0:name=tcp,bind="+host+",port="+strconv.Itoa(port)+",proto=tcp")

	return strings.Join(lines, "\r\n")
}

func (c Info) infoClients(w redis.Writer, snap redis.RuntimeSnapshot) string {
	cfg := redis.GetConfig(w)
	maxclients := 10000
	if cfg != nil && cfg.MaxClients > 0 {
		maxclients = cfg.MaxClients
	}

	var lines []string
	lines = append(lines, "# Clients")
	lines = append(lines, "connected_clients:"+strconv.FormatInt(snap.ConnectedClients, 10))
	lines = append(lines, "cluster_connections:0")
	lines = append(lines, "maxclients:"+strconv.Itoa(maxclients))
	lines = append(lines, "client_recent_max_input_buffer:0")
	lines = append(lines, "client_recent_max_output_buffer:0")
	lines = append(lines, "blocked_clients:0")
	lines = append(lines, "tracking_clients:0")
	lines = append(lines, "clients_in_timeout_table:0")
	lines = append(lines, "total_blocking_keys:0")
	lines = append(lines, "total_blocking_keys_on_nokey:0")
	return strings.Join(lines, "\r\n")
}

func (c Info) infoMemory(snap redis.RuntimeSnapshot) string {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	// Update peak memory tracking
	usedMemory := int64(ms.Alloc)
	usedMemoryRss := int64(ms.Sys)
	snap.PeakMemory = usedMemory

	// Get peak memory
	peakMemory := snap.PeakMemory
	if peakMemory < usedMemory {
		peakMemory = usedMemory
	}

	// Allocator stats - Go doesn't expose jemalloc-style metrics
	// We use Go runtime metrics as approximations
	allocatorAllocated := int64(ms.Alloc)
	allocatorActive := int64(ms.Alloc) // Approximation
	allocatorResident := int64(ms.Sys)

	// Overhead calculations
	overhead := usedMemoryRss - usedMemory
	if overhead < 0 {
		overhead = 0
	}
	datasetMemory := usedMemory

	// Fragmentation ratios
	allocatorFragRatio := float64(1)
	if allocatorActive > 0 {
		allocatorFragRatio = float64(allocatorAllocated) / float64(allocatorActive)
	}
	allocatorRssRatio := float64(1)
	if allocatorAllocated > 0 {
		allocatorRssRatio = float64(allocatorResident) / float64(allocatorAllocated)
	}

	// RSS overhead
	rssOverhead := usedMemoryRss - allocatorResident
	rssOverheadRatio := float64(1)
	if allocatorResident > 0 {
		rssOverheadRatio = float64(usedMemoryRss) / float64(allocatorResident)
	}

	memFragmentation := float64(1)
	if allocatorAllocated > 0 {
		memFragmentation = float64(usedMemoryRss) / float64(allocatorAllocated)
	}

	// Get client memory estimate
	connectedClients := int64(0)
	connectedClients = snap.ConnectedClients
	// Average per-client memory estimate: ~396KB for normal clients
	// This matches Redis's default client output buffer limit
	const perClientMemory int64 = 396 * 1024 // 396KB per client
	clientMem := connectedClients * perClientMemory

	var lines []string
	lines = append(lines, "# Memory")
	lines = append(lines, "used_memory:"+strconv.FormatInt(usedMemory, 10))
	lines = append(lines, "used_memory_human:"+formatBytes(usedMemory))
	lines = append(lines, "used_memory_rss:"+strconv.FormatInt(usedMemoryRss, 10))
	lines = append(lines, "used_memory_rss_human:"+formatBytes(usedMemoryRss))
	lines = append(lines, "used_memory_peak:"+strconv.FormatInt(peakMemory, 10))
	lines = append(lines, "used_memory_peak_human:"+formatBytes(peakMemory))
	lines = append(lines, "used_memory_peak_perc:"+percentage(uint64(usedMemory), uint64(peakMemory)))
	lines = append(lines, "used_memory_overhead:"+strconv.FormatInt(overhead, 10))
	lines = append(lines, "used_memory_startup:"+strconv.FormatInt(int64(ms.HeapIdle-ms.HeapReleased), 10))
	lines = append(lines, "used_memory_dataset:"+strconv.FormatInt(datasetMemory, 10))
	lines = append(lines, "used_memory_dataset_perc:"+percentage(uint64(datasetMemory), uint64(usedMemoryRss)))
	lines = append(lines, "allocator_allocated:"+strconv.FormatInt(allocatorAllocated, 10))
	lines = append(lines, "allocator_active:"+strconv.FormatInt(allocatorActive, 10))
	lines = append(lines, "allocator_resident:"+strconv.FormatInt(allocatorResident, 10))
	lines = append(lines, "total_system_memory:"+strconv.FormatUint(ms.Sys, 10))
	lines = append(lines, "total_system_memory_human:"+formatBytes(int64(ms.Sys)))
	lines = append(lines, "used_memory_lua:0")
	lines = append(lines, "used_memory_lua_human:0B")
	lines = append(lines, "used_memory_vm_eval:0")
	lines = append(lines, "used_memory_vm_total:0")
	lines = append(lines, "used_memory_vm_total_human:0B")
	lines = append(lines, "used_memory_functions:0")
	lines = append(lines, "used_memory_scripts:0")
	lines = append(lines, "used_memory_scripts_human:0B")
	lines = append(lines, "used_memory_scripts_eval:0")
	lines = append(lines, "number_of_cached_scripts:0")
	lines = append(lines, "number_of_functions:0")
	lines = append(lines, "number_of_libraries:0")
	lines = append(lines, "maxmemory:0")
	lines = append(lines, "maxmemory_human:0B")
	lines = append(lines, "maxmemory_policy:noeviction")
	lines = append(lines, "allocator_frag_ratio:"+strconv.FormatFloat(allocatorFragRatio, 'f', 2, 64))
	lines = append(lines, "allocator_frag_bytes:"+strconv.FormatInt(int64(float64(allocatorActive)*allocatorFragRatio)-allocatorActive, 10))
	lines = append(lines, "allocator_rss_ratio:"+strconv.FormatFloat(allocatorRssRatio, 'f', 2, 64))
	lines = append(lines, "allocator_rss_bytes:"+strconv.FormatInt(int64(float64(allocatorAllocated)*allocatorRssRatio)-allocatorAllocated, 10))
	lines = append(lines, "rss_overhead_ratio:"+strconv.FormatFloat(rssOverheadRatio, 'f', 2, 64))
	lines = append(lines, "rss_overhead_bytes:"+strconv.FormatInt(rssOverhead, 10))
	lines = append(lines, "mem_fragmentation_ratio:"+strconv.FormatFloat(memFragmentation, 'f', 2, 64))
	lines = append(lines, "mem_fragmentation_bytes:"+strconv.FormatInt(int64(float64(allocatorAllocated)*memFragmentation)-allocatorAllocated, 10))
	lines = append(lines, "mem_not_counted_for_evict:0")
	lines = append(lines, "mem_replication_backlog:0")
	lines = append(lines, "mem_total_replication_buffers:0")
	lines = append(lines, "mem_clients_slaves:0")
	lines = append(lines, "mem_clients_normal:"+strconv.FormatInt(clientMem, 10))
	lines = append(lines, "mem_cluster_links:0")
	lines = append(lines, "mem_aof_buffer:0")
	lines = append(lines, "mem_allocator:Go runtime allocator")
	lines = append(lines, "active_defrag_running:0")
	lines = append(lines, "lazyfree_pending_objects:0")
	lines = append(lines, "lazyfreed_objects:0")
	return strings.Join(lines, "\r\n")
}

func (c Info) infoPersistence() string {
	var lines []string
	lines = append(lines, "# Persistence")
	lines = append(lines, "loading:0")
	lines = append(lines, "async_loading:0")
	lines = append(lines, "current_cow_size:0")
	lines = append(lines, "current_cow_size_age:0")
	lines = append(lines, "current_fork_perc:0.00")
	lines = append(lines, "current_save_keys_processed:0")
	lines = append(lines, "current_save_keys_total:0")
	lines = append(lines, "rdb_changes_since_last_save:0")
	lines = append(lines, "rdb_bgsave_in_progress:0")
	lines = append(lines, "rdb_last_save_time:"+strconv.FormatInt(time.Now().Unix(), 10))
	lines = append(lines, "rdb_last_bgsave_status:ok")
	lines = append(lines, "rdb_last_bgsave_time_sec:0")
	lines = append(lines, "rdb_current_bgsave_time_sec:-1")
	lines = append(lines, "rdb_saves:0")
	lines = append(lines, "rdb_last_cow_size:0")
	lines = append(lines, "rdb_last_load_keys_expired:0")
	lines = append(lines, "rdb_last_load_keys_loaded:0")
	lines = append(lines, "aof_enabled:0")
	lines = append(lines, "aof_rewrite_in_progress:0")
	lines = append(lines, "aof_rewrite_scheduled:0")
	lines = append(lines, "aof_last_rewrite_time_sec:-1")
	lines = append(lines, "aof_current_rewrite_time_sec:-1")
	lines = append(lines, "aof_last_bgrewrite_status:ok")
	lines = append(lines, "aof_rewrites:0")
	lines = append(lines, "aof_rewrites_consecutive_failures:0")
	lines = append(lines, "aof_last_write_status:ok")
	lines = append(lines, "aof_last_cow_size:0")
	lines = append(lines, "module_fork_in_progress:0")
	lines = append(lines, "module_fork_last_cow_size:0")
	return strings.Join(lines, "\r\n")
}

func (c Info) infoStats(snap redis.RuntimeSnapshot) string {
	var lines []string
	lines = append(lines, "# Stats")
	lines = append(lines, "total_connections_received:"+strconv.FormatInt(snap.TotalConnectionsReceived, 10))
	lines = append(lines, "total_commands_processed:"+strconv.FormatInt(snap.TotalCommandsProcessed, 10))
	lines = append(lines, "instantaneous_ops_per_sec:"+strconv.FormatInt(snap.InstantaneousOpsPerSec, 10))
	lines = append(lines, "total_net_input_bytes:"+strconv.FormatInt(snap.TotalNetInputBytes, 10))
	lines = append(lines, "total_net_output_bytes:"+strconv.FormatInt(snap.TotalNetOutputBytes, 10))
	lines = append(lines, "total_net_repl_input_bytes:0")
	lines = append(lines, "total_net_repl_output_bytes:0")
	lines = append(lines, "instantaneous_input_kbps:0.00")
	lines = append(lines, "instantaneous_output_kbps:0.00")
	lines = append(lines, "instantaneous_input_repl_kbps:0.00")
	lines = append(lines, "instantaneous_output_repl_kbps:0.00")
	lines = append(lines, "rejected_connections:"+strconv.FormatInt(snap.RejectedConnections, 10))
	lines = append(lines, "sync_full:0")
	lines = append(lines, "sync_partial_ok:0")
	lines = append(lines, "sync_partial_err:0")
	lines = append(lines, "expired_keys:"+strconv.FormatInt(snap.ExpiredKeys, 10))
	lines = append(lines, "expired_stale_perc:0.00")
	lines = append(lines, "expired_time_cap_reached_count:0")
	lines = append(lines, "expire_cycle_cpu_milliseconds:0")
	lines = append(lines, "evicted_keys:"+strconv.FormatInt(snap.EvictedKeys, 10))
	lines = append(lines, "evicted_clients:0")
	lines = append(lines, "total_eviction_exceeded_time:0")
	lines = append(lines, "current_eviction_exceeded_time:0")
	lines = append(lines, "keyspace_hits:"+strconv.FormatInt(snap.KeyspaceHits, 10))
	lines = append(lines, "keyspace_misses:"+strconv.FormatInt(snap.KeyspaceMisses, 10))
	lines = append(lines, "pubsub_channels:0")
	lines = append(lines, "pubsub_patterns:0")
	lines = append(lines, "pubsubshard_channels:0")
	lines = append(lines, "latest_fork_usec:0")
	lines = append(lines, "total_forks:0")
	lines = append(lines, "migrate_cached_sockets:0")
	lines = append(lines, "slave_expires_tracked_keys:0")
	lines = append(lines, "active_defrag_hits:0")
	lines = append(lines, "active_defrag_misses:0")
	lines = append(lines, "active_defrag_key_hits:0")
	lines = append(lines, "active_defrag_key_misses:0")
	lines = append(lines, "total_active_defrag_time:0")
	lines = append(lines, "current_active_defrag_time:0")
	lines = append(lines, "tracking_total_keys:0")
	lines = append(lines, "tracking_total_items:0")
	lines = append(lines, "tracking_total_prefixes:0")
	lines = append(lines, "unexpected_error_replies:0")
	lines = append(lines, "total_error_replies:0")
	lines = append(lines, "dump_payload_sanitizations:0")
	lines = append(lines, "total_reads_processed:"+strconv.FormatInt(snap.TotalCommandsProcessed, 10))
	lines = append(lines, "total_writes_processed:"+strconv.FormatInt(snap.TotalCommandsProcessed, 10))
	lines = append(lines, "io_threaded_reads_processed:0")
	lines = append(lines, "io_threaded_writes_processed:0")
	lines = append(lines, "reply_buffer_shrinks:0")
	lines = append(lines, "reply_buffer_expands:0")
	lines = append(lines, "eventloop_cycles:0")
	lines = append(lines, "eventloop_duration_sum:0")
	lines = append(lines, "eventloop_duration_cmd_sum:0")
	lines = append(lines, "instantaneous_eventloop_cycles_per_sec:0")
	lines = append(lines, "instantaneous_eventloop_duration_usec:0")
	lines = append(lines, "acl_access_denied_auth:0")
	lines = append(lines, "acl_access_denied_cmd:0")
	lines = append(lines, "acl_access_denied_key:0")
	lines = append(lines, "acl_access_denied_channel:0")
	return strings.Join(lines, "\r\n")
}

func (c Info) infoReplication() string {
	var lines []string
	lines = append(lines, "# Replication")
	lines = append(lines, "role:master")
	lines = append(lines, "master_failover_state:no-failover")
	lines = append(lines, "master_replid:0000000000000000000000000000000000000000")
	lines = append(lines, "master_replid2:0000000000000000000000000000000000000000")
	lines = append(lines, "master_repl_offset:0")
	lines = append(lines, "second_repl_offset:-1")
	lines = append(lines, "repl_backlog_active:0")
	lines = append(lines, "repl_backlog_size:1048576")
	lines = append(lines, "repl_backlog_first_byte_offset:0")
	lines = append(lines, "repl_backlog_histlen:0")
	lines = append(lines, "connected_slaves:0")
	return strings.Join(lines, "\r\n")
}

func (c Info) infoCPU() string {
	var User float64   // user CPU time in seconds
	var System float64 // system CPU time in seconds

	pid := int32(os.Getpid())
	if p, err := process.NewProcess(pid); err == nil {
		if times, err := p.Times(); err == nil {
			User = times.User
			System = times.System
		}
	}

	var lines []string
	lines = append(lines, "# CPU")
	lines = append(lines, fmt.Sprintf("used_cpu_sys:%.6f", System))
	lines = append(lines, fmt.Sprintf("used_cpu_user:%.6f", User))
	lines = append(lines, "used_cpu_sys_children:0.000000")
	lines = append(lines, "used_cpu_user_children:0.000000")
	lines = append(lines, fmt.Sprintf("used_cpu_sys_main_thread:%.6f", System))
	lines = append(lines, fmt.Sprintf("used_cpu_user_main_thread:%.6f", User))
	return strings.Join(lines, "\r\n")
}

func (c Info) infoModules(red redis.Redka) string {
	var lines []string
	lines = append(lines, "# Modules")

	s := red.Store()
	ver, _ := s.Version()
	lines = append(lines, fmt.Sprintf("module:name=%s,version=%s", s.Dialect, ver))

	return strings.Join(lines, "\r\n")
}

func (c Info) infoCluster() string {
	return "# Cluster\r\ncluster_enabled:0"
}

func (c Info) infoErrorStats(snap redis.RuntimeSnapshot) string {
	var lines []string
	lines = append(lines, "# Errorstats")
	for errType, count := range snap.ErrorStats {
		lines = append(lines, fmt.Sprintf("errorstat_%s:count=%d", errType, count))
	}
	return strings.Join(lines, "\r\n")
}

func (c Info) infoKeyspace(w redis.Writer, red redis.Redka) string {
	var lines []string
	lines = append(lines, "# Keyspace")

	cfg := redis.GetConfig(w)
	dbCount := 16
	if cfg != nil && cfg.Databases > 0 {
		dbCount = cfg.Databases
	}

	s := red.Store()
	now := time.Now().UnixMilli()

	// Report all databases
	for dbIdx := 0; dbIdx < dbCount; dbIdx++ {
		var keyCount, expires int64
		var ttlSum int64

		// Count total keys in this DB
		s.DB.Model(&store.RKey{}).Where("kdb = ?", dbIdx).Count(&keyCount)
		if keyCount == 0 {
			continue
		}

		// Count keys with expiration
		s.DB.Model(&store.RKey{}).
			Where("kdb = ? AND expire_at IS NOT NULL AND expire_at > ?", dbIdx, now).
			Count(&expires)

		// Sum TTL for average calculation
		var expireAts []int64
		s.DB.Model(&store.RKey{}).
			Where("kdb = ? AND expire_at IS NOT NULL AND expire_at > ?", dbIdx, now).
			Pluck("expire_at", &expireAts)
		for _, t := range expireAts {
			ttlSum += t - now
		}

		avgTTL := int64(0)
		if expires > 0 {
			avgTTL = ttlSum / expires
		}

		lines = append(lines, fmt.Sprintf("db%d:keys=%d,expires=%d,avg_ttl=%d", dbIdx, keyCount, expires, avgTTL))
	}

	if len(lines) == 1 {
		return "# Keyspace"
	}
	return strings.Join(lines, "\r\n")
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
	return infoAllSections()
}

func infoRuntimeSnapshot(w redis.Writer) redis.RuntimeSnapshot {
	stats := redis.GetRuntimeStats(w)
	if stats == nil {
		return redis.RuntimeSnapshot{}
	}
	return stats.Snapshot(time.Now())
}

// --- Helper functions ---

func formatBytes(n int64) string {
	if n <= 0 {
		return "0B"
	}
	const unit = int64(1024)
	if n < unit {
		return strconv.FormatInt(n, 10) + "B"
	}
	div, _ := divMod(n, unit)
	if div < unit {
		return strconv.FormatInt(div, 10) + "K"
	}
	div, _ = divMod(div, unit)
	if div < unit {
		return strconv.FormatInt(div, 10) + "M"
	}
	div, _ = divMod(div, unit)
	return strconv.FormatInt(div, 10) + "G"
}

func divMod(a, b int64) (int64, int64) {
	return a / b, a % b
}

func percentage(a, b uint64) string {
	if b == 0 {
		return "0.00%"
	}
	return strconv.FormatFloat(float64(a)/float64(b)*100, 'f', 2, 64) + "%"
}
