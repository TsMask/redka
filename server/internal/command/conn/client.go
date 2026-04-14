package conn

import (
	"strconv"
	"strings"

	"github.com/tsmask/redka/server/internal/redis"
)

// Client is a container command for CLIENT subcommands.
// CLIENT <subcommand> [<arg> ...]
// https://redis.io/commands/client-list
type Client struct {
	redis.BaseCmd
	subcmd string
	args   [][]byte
}

func ParseClient(b redis.BaseCmd) (Client, error) {
	cmd := Client{BaseCmd: b}
	args := cmd.Args()
	if len(args) == 0 {
		return Client{}, redis.ErrInvalidArgNum
	}
	cmd.subcmd = strings.ToUpper(string(args[0]))
	cmd.args = args[1:]
	return cmd, nil
}

func (c Client) Run(w redis.Writer, _ redis.Redka) (any, error) {
	// CLIENT subcommands dispatch
	switch c.subcmd {
	case "LIST":
		return c.runClientList(w)
	case "KILL":
		return c.runClientKill(w)
	case "GETNAME":
		return c.runClientGetName(w)
	case "SETNAME":
		return c.runClientSetName(w)
	case "ID":
		return c.runClientID(w)
	case "INFO":
		return c.runClientInfo(w)
	case "PAUSE":
		return c.runClientPause(w)
	case "UNPAUSE":
		return c.runClientUnpause(w)
	case "REPLY":
		return c.runClientReply(w)
	case "TRACKING":
		return c.runClientTracking(w)
	case "TRACKINGINFO":
		return c.runClientTrackingInfo(w)
	case "UNBLOCK":
		return c.runClientUnblock(w)
	case "CACHING":
		return c.runClientCaching(w)
	case "GETREDIR":
		return c.runClientGetRedir(w)
	case "NO-EVICT":
		return c.runClientNoEvict(w)
	case "NO-TOUCH":
		return c.runClientNoTouch(w)
	case "SETINFO":
		return c.runClientSetInfo(w)
	default:
		w.WriteError("ERR Unknown subcommand or wrong number of arguments")
		return nil, nil
	}
}

// CLIENT LIST — Returns information and statistics about connected clients.
func (c Client) runClientList(w redis.Writer) (any, error) {
	registry := redis.GetClientRegistryFromWriter(w)
	if registry == nil {
		registry = redis.GetClientRegistry()
	}

	// Check for TYPE filter
	for i := 0; i < len(c.args); i++ {
		if strings.ToUpper(string(c.args[i])) == "TYPE" && i+1 < len(c.args) {
			// Filter by type (we only support NORMAL for now)
			typ := strings.ToUpper(string(c.args[i+1]))
			if typ != "NORMAL" && typ != "PUBSUB" {
				w.WriteBulkString("")
				return "", nil
			}
		}
	}

	infos := registry.List()
	lines := make([]string, 0, len(infos))
	for _, info := range infos {
		lines = append(lines, redis.FormatClientInfo(info))
	}
	text := strings.Join(lines, "\n")
	w.WriteBulkString(text)
	return text, nil
}

// CLIENT KILL — Closes client connections.
func (c Client) runClientKill(w redis.Writer) (any, error) {
	registry := redis.GetClientRegistryFromWriter(w)
	if registry == nil {
		registry = redis.GetClientRegistry()
	}

	args := c.args
	if len(args) == 0 {
		return nil, redis.ErrInvalidArgNum
	}

	// Old format: CLIENT KILL <ip:port>
	if len(args) == 1 && !strings.Contains(string(args[0]), "=") {
		addr := string(args[0])
		count := registry.CloseByAddr(addr)
		w.WriteInt(count)
		return count, nil
	}

	// New format: CLIENT KILL <filter> <value> ...
	killed := 0
	for i := 0; i < len(args); i++ {
		filter := strings.ToUpper(string(args[i]))
		switch filter {
		case "ID":
			if i+1 >= len(args) {
				return nil, redis.ErrInvalidArgNum
			}
			id, err := strconv.ParseInt(string(args[i+1]), 10, 64)
			if err != nil {
				w.WriteError("ERR Invalid client id")
				return nil, nil
			}
			i++
			if registry.Close(id) {
				killed++
			}
		case "ADDR":
			if i+1 >= len(args) {
				return nil, redis.ErrInvalidArgNum
			}
			i++
			killed += registry.CloseByAddr(string(args[i]))
		case "SKIPME":
			if i+1 >= len(args) {
				return nil, redis.ErrInvalidArgNum
			}
			i++
			// Skipme doesn't affect anything in single-client context
		case "TYPE":
			if i+1 >= len(args) {
				return nil, redis.ErrInvalidArgNum
			}
			i++
			// We don't distinguish client types, so this is a no-op
		default:
			w.WriteError("ERR Unknown filter type")
			return nil, nil
		}
	}

	w.WriteInt(killed)
	return killed, nil
}

// CLIENT GETNAME — Returns the name of the current connection.
func (c Client) runClientGetName(w redis.Writer) (any, error) {
	name := redis.GetClientName(w)
	if name == "" {
		w.WriteNull()
		return nil, nil
	}
	w.WriteBulkString(name)
	return name, nil
}

// CLIENT SETNAME — Sets the name of the current connection.
func (c Client) runClientSetName(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}
	name := string(c.args[0])
	redis.SetClientName(w, name)

	// Also update in registry
	registry := redis.GetClientRegistryFromWriter(w)
	if registry != nil {
		if id := redis.GetConnID(w); id != 0 {
			registry.SetConnInfo(id, name, "", "")
		}
	}

	w.WriteString("OK")
	return true, nil
}

// CLIENT ID — Returns the current connection ID.
func (c Client) runClientID(w redis.Writer) (any, error) {
	id := redis.GetConnID(w)
	w.WriteInt64(id)
	return id, nil
}

// CLIENT INFO — Returns information about the current client connection.
func (c Client) runClientInfo(w redis.Writer) (any, error) {
	registry := redis.GetClientRegistryFromWriter(w)
	if registry == nil {
		registry = redis.GetClientRegistry()
	}

	id := redis.GetConnID(w)
	info, ok := registry.Get(id)
	if !ok {
		// Return minimal info
		info = redis.ClientInfo{
			ID:     id,
			Addr:   "",
			Flags:  "N",
			Events: "rw",
		}
	}

	// Ensure fields are populated from current state
	info.Name = redis.GetClientName(w)
	info.DB = redis.GetSelectedDB(w)

	// Get resp from protover
	info.Resp = redis.GetProtover(w)
	if info.Resp == 0 {
		info.Resp = 2
	}

	text := redis.FormatClientInfo(info)
	w.WriteBulkString(text)
	return text, nil
}

// CLIENT PAUSE — Suspends all Redis clients for the specified time.
func (c Client) runClientPause(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	ms, err := strconv.ParseInt(string(c.args[0]), 10, 64)
	if err != nil {
		w.WriteError("ERR Invalid timeout")
		return nil, nil
	}

	// Store pause state in connection context
	redis.SetClientPaused(w, true)
	if len(c.args) >= 2 {
		mode := strings.ToUpper(string(c.args[1]))
		redis.SetClientPauseMode(w, mode)
	} else {
		redis.SetClientPauseMode(w, "ALL")
	}

	_ = ms // Actual pause implementation would need timer support
	w.WriteString("OK")
	return true, nil
}

// CLIENT UNPAUSE — Resumes all suspended clients.
func (c Client) runClientUnpause(w redis.Writer) (any, error) {
	redis.SetClientPaused(w, false)
	w.WriteString("OK")
	return true, nil
}

// CLIENT REPLY — Controls whether the server will reply to commands.
func (c Client) runClientReply(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	mode := strings.ToUpper(string(c.args[0]))
	switch mode {
	case "ON":
		redis.SetClientReplyMode(w, "ON")
		w.WriteString("OK")
	case "OFF":
		redis.SetClientReplyMode(w, "OFF")
		// Don't write anything back
	case "SKIP":
		redis.SetClientReplyMode(w, "SKIP")
		// Next command reply will be skipped
		redis.SkipNextReply(w)
	default:
		w.WriteError("ERR Invalid reply mode")
	}
	return nil, nil
}

// CLIENT TRACKING — Enables server-assisted client-side caching.
func (c Client) runClientTracking(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	mode := strings.ToUpper(string(c.args[0]))
	switch mode {
	case "ON":
		redis.SetClientTracking(w, true)
		w.WriteString("OK")
	case "OFF":
		redis.SetClientTracking(w, false)
		w.WriteString("OK")
	default:
		w.WriteError("ERR Invalid tracking mode")
	}
	return nil, nil
}

// CLIENT TRACKINGINFO — Returns information about server-assisted client-side caching.
func (c Client) runClientTrackingInfo(w redis.Writer) (any, error) {
	tracking := redis.IsClientTracking(w)
	w.WriteArray(6)
	w.WriteBulkString("tracking")
	if tracking {
		w.WriteInt(1)
	} else {
		w.WriteInt(0)
	}
	w.WriteBulkString("tracking_clients")
	w.WriteInt(0)
	w.WriteBulkString("prefixes")
	w.WriteArray(0)
	w.WriteBulkString("enable_tracking_commands")
	w.WriteArray(0)
	w.WriteBulkString("last_error")
	w.WriteBulkString("")
	w.WriteBulkString("redirected_client_id")
	w.WriteInt64(0)
	return nil, nil
}

// CLIENT UNBLOCK — Unblocks a client blocked by a blocking command.
func (c Client) runClientUnblock(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	id, err := strconv.ParseInt(string(c.args[0]), 10, 64)
	if err != nil {
		w.WriteError("ERR Invalid client id")
		return nil, nil
	}

	reason := "TIMEOUT"
	if len(c.args) >= 2 {
		reason = strings.ToUpper(string(c.args[1]))
	}

	// Try to find and unblock the client
	registry := redis.GetClientRegistryFromWriter(w)
	if registry == nil {
		w.WriteInt(0)
		return 0, nil
	}

	// In a real implementation, we'd track blocked clients and unblock them
	_ = id
	_ = reason
	w.WriteInt(0)
	return 0, nil
}

// CLIENT CACHING — Hints to the server about whether the client intends to cache.
func (c Client) runClientCaching(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	mode := strings.ToUpper(string(c.args[0]))
	switch mode {
	case "YES":
		redis.SetClientCaching(w, true)
	case "NO":
		redis.SetClientCaching(w, false)
	default:
		w.WriteError("ERR Invalid caching mode")
		return nil, nil
	}

	w.WriteString("OK")
	return true, nil
}

// CLIENT GETREDIR — Returns the client ID to which notifications are redirected.
func (c Client) runClientGetRedir(w redis.Writer) (any, error) {
	redir := redis.GetClientTrackingRedir(w)
	w.WriteInt64(redir)
	return redir, nil
}

// CLIENT NO-EVICT — Sets the client to no-eviction mode.
func (c Client) runClientNoEvict(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	mode := strings.ToUpper(string(c.args[0]))
	switch mode {
	case "ON":
		redis.SetClientNoEvict(w, true)
	case "OFF":
		redis.SetClientNoEvict(w, false)
	default:
		w.WriteError("ERR Invalid no-evict mode")
		return nil, nil
	}

	w.WriteString("OK")
	return true, nil
}

// CLIENT NO-TOUCH — Sets the client to no-touch mode.
func (c Client) runClientNoTouch(w redis.Writer) (any, error) {
	if len(c.args) == 0 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	mode := strings.ToUpper(string(c.args[0]))
	switch mode {
	case "ON":
		redis.SetClientNoTouch(w, true)
	case "OFF":
		redis.SetClientNoTouch(w, false)
	default:
		w.WriteError("ERR Invalid no-touch mode")
		return nil, nil
	}

	w.WriteString("OK")
	return true, nil
}

// CLIENT SETINFO — Sets information specific to the client.
func (c Client) runClientSetInfo(w redis.Writer) (any, error) {
	if len(c.args) < 2 {
		w.WriteError("ERR Invalid arguments")
		return nil, nil
	}

	key := strings.ToUpper(string(c.args[0]))
	value := string(c.args[1])

	registry := redis.GetClientRegistryFromWriter(w)
	id := redis.GetConnID(w)

	switch key {
	case "LIB-NAME":
		if registry != nil && id != 0 {
			registry.SetConnInfo(id, "", value, "")
		}
		redis.SetClientLibName(w, value)
	case "LIB-VER":
		if registry != nil && id != 0 {
			registry.SetConnInfo(id, "", "", value)
		}
		redis.SetClientLibVer(w, value)
	default:
		// Allow any key-value pair
		redis.SetClientInfo(w, key, value)
	}

	w.WriteString("OK")
	return true, nil
}
