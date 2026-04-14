// Package memory implements Redis MEMORY commands.
// https://redis.io/commands/?group=memory
package memory

import (
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"

	"github.com/tsmask/redka/internal/core"
	"github.com/tsmask/redka/server/internal/redis"
)

// Memory is a container command for MEMORY subcommands.
// MEMORY <subcommand> [<arg> ...]
type Memory struct {
	redis.BaseCmd
	subcmd string
	args   [][]byte
}

func ParseMemory(b redis.BaseCmd) (Memory, error) {
	cmd := Memory{BaseCmd: b}
	args := cmd.Args()
	if len(args) == 0 {
		return Memory{}, redis.ErrInvalidArgNum
	}
	cmd.subcmd = strings.ToUpper(string(args[0]))
	cmd.args = args[1:]
	return cmd, nil
}

func (c Memory) Run(w redis.Writer, red redis.Redka) (any, error) {
	switch c.subcmd {
	case "STATS":
		return c.runMemoryStats(w, red)
	case "PURGE":
		return c.runMemoryPurge(w, red)
	case "USAGE":
		return c.runMemoryUsage(w, red)
	default:
		w.WriteError("ERR Unknown subcommand or wrong number of arguments")
		return nil, nil
	}
}

// runMemoryStats — MEMORY STATS
func (c Memory) runMemoryStats(w redis.Writer, red redis.Redka) (any, error) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	keyCount, _ := red.Key().Len()
	datasetBytes := ms.Alloc

	w.WriteArray(34)
	w.WriteBulkString("peak.allocated")
	w.WriteUint64(ms.Alloc)
	w.WriteBulkString("total.allocated")
	w.WriteUint64(ms.Alloc)
	w.WriteBulkString("startup.allocated")
	w.WriteUint64(ms.Sys)
	w.WriteBulkString("replication.backlog")
	w.WriteInt(0)
	w.WriteBulkString("clients.slaves")
	w.WriteInt(0)
	w.WriteBulkString("clients.normal")
	w.WriteUint64(ms.Alloc / 10)
	w.WriteBulkString("cluster.links")
	w.WriteInt(0)
	w.WriteBulkString("aof.buffer")
	w.WriteInt(0)
	w.WriteBulkString("lua.caches")
	w.WriteInt(0)
	w.WriteBulkString("functions.caches")
	w.WriteInt(0)
	w.WriteBulkString("overhead.db.hashtable.lut")
	w.WriteUint64(uint64(keyCount) * 48)
	w.WriteBulkString("overhead.db.hashtable.rehashing")
	w.WriteInt(0)
	w.WriteBulkString("overhead.total")
	w.WriteUint64(ms.Sys - ms.Alloc)
	w.WriteBulkString("db.dict.rehashing.count")
	w.WriteInt(0)
	w.WriteBulkString("keys.count")
	w.WriteUint64(uint64(keyCount))
	if keyCount > 0 {
		w.WriteBulkString("keys.bytes-per-key")
		w.WriteUint64(datasetBytes / uint64(keyCount))
	} else {
		w.WriteBulkString("keys.bytes-per-key")
		w.WriteInt(0)
	}
	w.WriteBulkString("dataset.bytes")
	w.WriteUint64(datasetBytes)
	if ms.Sys > 0 {
		w.WriteBulkString("dataset.percentage")
		w.WriteBulkString(strconv.FormatFloat(float64(datasetBytes)/float64(ms.Sys)*100, 'f', 2, 64))
	} else {
		w.WriteBulkString("dataset.percentage")
		w.WriteBulkString("0.00")
	}
	w.WriteBulkString("peak.percentage")
	w.WriteBulkString("100.00")
	w.WriteBulkString("allocator.allocated")
	w.WriteUint64(ms.Alloc)
	w.WriteBulkString("allocator.active")
	w.WriteUint64(ms.Alloc)
	w.WriteBulkString("allocator.resident")
	w.WriteUint64(ms.Sys)
	w.WriteBulkString("allocator.muzzy")
	w.WriteInt(0)
	denom := ms.Sys
	if denom < ms.Alloc {
		denom = ms.Alloc
	}
	w.WriteBulkString("allocator-fragmentation.ratio")
	w.WriteBulkString(strconv.FormatFloat(float64(ms.Alloc)/float64(denom), 'f', 2, 64))
	if ms.Sys > ms.Alloc {
		w.WriteBulkString("allocator-fragmentation.bytes")
		w.WriteUint64(ms.Sys - ms.Alloc)
	} else {
		w.WriteBulkString("allocator-fragmentation.bytes")
		w.WriteInt(0)
	}
	if ms.Alloc == 0 {
		w.WriteBulkString("allocator-rss.ratio")
		w.WriteBulkString("1.00")
	} else {
		w.WriteBulkString("allocator-rss.ratio")
		w.WriteBulkString(strconv.FormatFloat(float64(ms.Sys)/float64(ms.Alloc), 'f', 2, 64))
	}
	w.WriteBulkString("allocator-rss.bytes")
	w.WriteUint64(ms.Sys)
	w.WriteBulkString("rss-overhead.ratio")
	w.WriteBulkString("1.00")
	w.WriteBulkString("rss-overhead.bytes")
	w.WriteInt(0)
	w.WriteBulkString("fragmentation")
	w.WriteBulkString(strconv.FormatFloat(float64(ms.Alloc)/float64(denom), 'f', 2, 64))
	if ms.Sys > ms.Alloc {
		w.WriteBulkString("fragmentation.bytes")
		w.WriteUint64(ms.Sys - ms.Alloc)
	} else {
		w.WriteBulkString("fragmentation.bytes")
		w.WriteInt(0)
	}

	return nil, nil
}

// runMemoryPurge — MEMORY PURGE
func (c Memory) runMemoryPurge(w redis.Writer, red redis.Redka) (any, error) {
	runtime.GC()
	debug.FreeOSMemory()
	w.WriteString("OK")
	return true, nil
}

// runMemoryUsage — MEMORY USAGE key [SAMPLES count]
func (c Memory) runMemoryUsage(w redis.Writer, red redis.Redka) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	key := string(c.args[0])

	// Parse optional SAMPLES count (ignored for now, we use Len)
	_ = 5 // default sample count, not used in estimate
	for i := 1; i < len(c.args); i++ {
		if strings.ToUpper(string(c.args[i])) == "SAMPLES" && i+1 < len(c.args) {
			i++
		}
	}

	k, err := red.Key().Get(key)
	if err == core.ErrNotFound {
		w.WriteNull()
		return nil, nil
	}
	if err != nil {
		w.WriteError(c.BaseCmd.Error(err))
		return nil, err
	}

	usage := estimateKeyUsage(k)
	w.WriteInt64(usage)
	return usage, nil
}

func estimateKeyUsage(k core.Key) int64 {
	base := int64(56) // key metadata overhead

	switch k.Type {
	case core.TypeString:
		base += int64(k.Len)
	case core.TypeHash:
		base += int64(k.Len) * 48
	case core.TypeList:
		base += int64(k.Len) * 56
	case core.TypeSet:
		base += int64(k.Len) * 48
	case core.TypeZSet:
		base += int64(k.Len) * 64
	}

	return base
}
