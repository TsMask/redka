#!/usr/bin/env bash
#
# Benchmarks every top-level command registered in
# server/internal/command/command.go with redis-benchmark.
#
# Usage:
#   ./test_redis-benchmark.sh [host] [port] [password] [requests] [clients] [report]
#
# The server must require authentication. Start it for example with:
#   redka -h 127.0.0.1 -p 6380 -a benchmark-secret :memory:

set -euo pipefail

HOST=${1:-127.0.0.1}
PORT=${2:-6380}
PASSWORD=${3:-benchmark-secret}
REQUESTS=${4:-1000}
CLIENTS=${5:-10}
PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
REPORT=${6:-$PROJECT_ROOT/redis-benchmark-report.txt}

if [[ -z "$PASSWORD" ]]; then
    echo "A password is required so AUTH can be tested." >&2
    exit 2
fi

for tool in redis-benchmark redis-cli; do
    if ! command -v "$tool" >/dev/null 2>&1; then
        echo "Missing required tool: $tool" >&2
        exit 2
    fi
done

CLI=(redis-cli -h "$HOST" -p "$PORT" -a "$PASSWORD" --no-auth-warning)
BENCH=(redis-benchmark -h "$HOST" -p "$PORT" -a "$PASSWORD"
    -n "$REQUESTS" -c "$CLIENTS" -e)

EXPECTED_COMMANDS=(
    command config dbsize flushdb flushall save info lolwut slowlog memory
    auth client echo hello ping select
    del exists expire expireat keys persist pexpire pexpireat pttl randomkey
    rename renamenx scan ttl type
    lindex linsert llen lpop lpush lrange lrem lset ltrim rpop rpoplpush rpush
    append decr decrby get getrange getset incr incrby incrbyfloat mget mset
    psetex set setex setnx setrange strlen substr
    hdel hexists hget hgetall hincrby hincrbyfloat hkeys hlen hmget hmset hscan
    hset hsetnx hrandfield hstrlen hvals
    sadd scard sdiff sdiffstore sinter sintercard sinterstore sismember
    smembers smismember smove spop srandmember srem sscan sunion sunionstore
    zadd zcard zcount zincrby zinter zinterstore zrange zrangebyscore zrank zrem
    zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zrevrank zscan
    zscore zunion zunionstore
)

declare -A COVERED_COMMANDS=()
declare -a SUMMARY_LINES=()
PASS_COUNT=0
FAIL_COUNT=0

die() {
    echo "ERROR: $*" >&2
    exit 2
}

run() {
    if ! "${CLI[@]}" "$@" >/dev/null; then
        die "seed command failed: $*"
    fi
}

benchmark() {
    local name=${1,,}
    local output status=pass rps=""

    COVERED_COMMANDS["$name"]=1
    if ! output=$("${BENCH[@]}" "$@" 2>&1); then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        status=fail
        echo "FAIL $name"
    elif [[ $output == *"Error from server:"* || $output == *"replied with error:"* ]]; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        status=fail
        echo "FAIL $name"
    fi

    rps=$(printf '%s\n' "$output" | awk '/requests per second/ {rps=$1} END {print rps}')
    {
        printf '\n## %s\n\n' "$(printf '%s' "$*" | sed 's/[[:cntrl:]]//g')"
        printf 'Result: %s\n\n' "$status"
        printf '%s\n' "$output" | sed 's/\r$//'
    } >> "$REPORT"
    SUMMARY_LINES+=("| $name | $rps | $status |")

    if [[ $status == fail ]]; then
        return
    fi

    PASS_COUNT=$((PASS_COUNT + 1))
    echo "PASS $name ($rps requests/sec)"
}

check_prerequisites() {
    if ! "${CLI[@]}" PING >/dev/null 2>&1; then
        die "cannot authenticate to Redka at $HOST:$PORT"
    fi
}

seed_data() {
    run FLUSHALL
    consume_seed=$((REQUESTS + 3))

    # Strings and keys.
    run SET benchmark:string "hello world"
    run SETEX benchmark:ttl 120 value
    run SET benchmark:persist "persist me"
    run EXPIRE benchmark:persist 120
    run SET benchmark:expire "expire me"
    run SET benchmark:expireat "expire at"
    run SET benchmark:pexpire "pexpire me"
    run SET benchmark:pexpireat "pexpire at"
    run SET benchmark:rename-src "rename source"
    run SET benchmark:renamenx-src "renamenx source"
    run SET benchmark:renamenx-dst "renamenx destination"
    run SET benchmark:del "delete me"
    run SET benchmark:getset "old value"
    run SET benchmark:counter 10
    run SET benchmark:float-counter 1.5
    run SET benchmark:append "hello"

    # Hashes.
    run HSET benchmark:hash name Alice age 30 city Beijing extra delete-me

    # Lists.
    run RPUSH benchmark:list a b c d
    run RPUSH benchmark:lset x y z
    run RPUSH benchmark:linsert a b c
    run RPUSH benchmark:lrem a b c
    run RPUSH benchmark:lpop a b c
    run RPUSH benchmark:lpop $(seq 1 "$consume_seed")
    run RPUSH benchmark:rpop a b c
    run RPUSH benchmark:rpop $(seq 1 "$consume_seed")
    run RPUSH benchmark:rpoplpush-src a b c
    run RPUSH benchmark:rpoplpush-src $(seq 1 "$consume_seed")
    run RPUSH benchmark:rpoplpush-dst x
    run RPUSH benchmark:ltrim a b c d

    # Sets.
    run SADD benchmark:set a b c d
    run SADD benchmark:set-a a b c
    run SADD benchmark:set-b b c d
    run SADD benchmark:set-write x
    run SADD benchmark:spop x y z
    run SADD benchmark:spop $(seq 1 "$consume_seed")
    run SADD benchmark:srem x y z
    run SADD benchmark:smove-src x y z
    run SADD benchmark:smove-dst destination
    run SADD benchmark:sinterstore-src a b
    run SADD benchmark:sinterstore-dst b c
    run SADD benchmark:sunionstore-src a b
    run SADD benchmark:sunionstore-dst c d
    run SADD benchmark:sdiffstore-src a b
    run SADD benchmark:sdiffstore-dst b c

    # Sorted sets.
    run ZADD benchmark:zset 1 one 2 two 3 three 4 four 5 five
    run ZADD benchmark:zset-a 1 a 2 b 3 c
    run ZADD benchmark:zset-b 2 b 3 c 4 d
    run ZADD benchmark:zset-write 1 member
    run ZADD benchmark:zset-incr 1 member
    run ZADD benchmark:zset-rem 1 a 2 b
    run ZADD benchmark:zset-rem-rank 1 a 2 b 3 c
    run ZADD benchmark:zset-rem-score 1 a 2 b 3 c
}

check_prerequisites
seed_data

future_s=$(( $(date +%s) + 3600 ))
future_ms=$(( future_s * 1000 ))

: > "$REPORT"
{
    printf '# Redka redis-benchmark Report\n\n'
    printf -- '- Date: %s\n' "$(date -u '+%Y-%m-%d %H:%M:%S UTC')"
    printf -- '- Target: %s:%s\n' "$HOST" "$PORT"
    printf -- '- Requests per command: %s\n' "$REQUESTS"
    printf -- '- Parallel clients: %s\n' "$CLIENTS"
    printf -- '- Registered commands: %s\n\n' "${#EXPECTED_COMMANDS[@]}"
} > "$REPORT"

echo "Benchmarking ${#EXPECTED_COMMANDS[@]} registered commands at $HOST:$PORT"
echo "Report: $REPORT"

# Server and connection commands.
benchmark COMMAND DOCS
benchmark CONFIG GET requirepass
benchmark DBSIZE
benchmark SAVE
benchmark INFO server
benchmark LOLWUT
benchmark SLOWLOG LEN
benchmark MEMORY USAGE benchmark:string
benchmark AUTH "$PASSWORD"
benchmark CLIENT LIST
benchmark ECHO benchmark
benchmark HELLO 3
benchmark PING benchmark
benchmark SELECT 0

# Key commands. Destructive operations use isolated seeded keys.
benchmark TYPE benchmark:string
benchmark EXISTS benchmark:string
benchmark EXPIRE benchmark:expire 120
benchmark EXPIREAT benchmark:expireat "$future_s"
benchmark KEYS "benchmark:string"
benchmark PERSIST benchmark:persist
benchmark PEXPIRE benchmark:pexpire 120000
benchmark PEXPIREAT benchmark:pexpireat "$future_ms"
benchmark TTL benchmark:ttl
benchmark PTTL benchmark:ttl
benchmark RANDOMKEY
benchmark RENAME benchmark:rename-src benchmark:rename-src
benchmark RENAMENX benchmark:renamenx-src benchmark:renamenx-src
benchmark SCAN 0
benchmark DEL benchmark:del

# String commands.
benchmark GET benchmark:string
benchmark GETRANGE benchmark:string 0 4
benchmark SUBSTR benchmark:string 6 10
benchmark STRLEN benchmark:string
benchmark APPEND benchmark:append " world"
benchmark GETSET benchmark:getset "new value"
benchmark SET benchmark:string-write "new value"
benchmark SETEX benchmark:setex 120 value
benchmark PSETEX benchmark:psetex 120000 value
benchmark SETNX benchmark:setnx value
benchmark SETRANGE benchmark:setrange 6 world
benchmark MSET benchmark:mset-a a benchmark:mset-b b
benchmark MGET benchmark:mset-a benchmark:mset-b
benchmark INCR benchmark:counter
benchmark INCRBY benchmark:counter 5
benchmark DECR benchmark:counter
benchmark DECRBY benchmark:counter 2
benchmark INCRBYFLOAT benchmark:float-counter 2.5

# Hash commands.
benchmark HGET benchmark:hash name
benchmark HMGET benchmark:hash name age
benchmark HGETALL benchmark:hash
benchmark HEXISTS benchmark:hash name
benchmark HLEN benchmark:hash
benchmark HKEYS benchmark:hash
benchmark HVALS benchmark:hash
benchmark HSTRLEN benchmark:hash name
benchmark HRANDFIELD benchmark:hash
benchmark HSCAN benchmark:hash 0
benchmark HSET benchmark:hash country China
benchmark HSETNX benchmark:hash language Go
benchmark HMSET benchmark:hash role engineer
benchmark HINCRBY benchmark:hash age 1
benchmark HINCRBYFLOAT benchmark:hash age 0.5
benchmark HDEL benchmark:hash extra

# List commands.
benchmark LRANGE benchmark:list 0 -1
benchmark LLEN benchmark:list
benchmark LINDEX benchmark:list 0
benchmark LSET benchmark:lset 1 updated
benchmark LINSERT benchmark:linsert before a inserted
benchmark LREM benchmark:lrem 1 a
benchmark LPOP benchmark:lpop
benchmark RPOP benchmark:rpop
benchmark RPOPLPUSH benchmark:rpoplpush-src benchmark:rpoplpush-dst
benchmark LTRIM benchmark:ltrim 1 2
benchmark LPUSH benchmark:lpush-write element
benchmark RPUSH benchmark:rpush-write element

# Set commands.
benchmark SMEMBERS benchmark:set
benchmark SCARD benchmark:set
benchmark SISMEMBER benchmark:set a
benchmark SMISMEMBER benchmark:set a missing
benchmark SRANDMEMBER benchmark:set
benchmark SSCAN benchmark:set 0
benchmark SINTER benchmark:set-a benchmark:set-b
benchmark SUNION benchmark:set-a benchmark:set-b
benchmark SDIFF benchmark:set-a benchmark:set-b
benchmark SINTERCARD 2 benchmark:set-a benchmark:set-b
benchmark SADD benchmark:set-write another
benchmark SPOP benchmark:spop
benchmark SREM benchmark:srem x
benchmark SMOVE benchmark:smove-src benchmark:smove-dst x
benchmark SINTERSTORE benchmark:sinter-result \
    benchmark:sinterstore-src benchmark:sinterstore-dst
benchmark SUNIONSTORE benchmark:sunion-result \
    benchmark:sunionstore-src benchmark:sunionstore-dst
benchmark SDIFFSTORE benchmark:sdiff-result \
    benchmark:sdiffstore-src benchmark:sdiffstore-dst

# Sorted-set commands.
benchmark ZRANGE benchmark:zset 0 -1
benchmark ZREVRANGE benchmark:zset 0 -1
benchmark ZRANGEBYSCORE benchmark:zset 2 4
benchmark ZREVRANGEBYSCORE benchmark:zset 4 2
benchmark ZSCORE benchmark:zset one
benchmark ZRANK benchmark:zset one
benchmark ZREVRANK benchmark:zset one
benchmark ZCOUNT benchmark:zset 2 4
benchmark ZCARD benchmark:zset
benchmark ZSCAN benchmark:zset 0
benchmark ZINTER 2 benchmark:zset-a benchmark:zset-b
benchmark ZUNION 2 benchmark:zset-a benchmark:zset-b
benchmark ZADD benchmark:zset-write 2 another
benchmark ZINCRBY benchmark:zset-incr 2 member
benchmark ZREM benchmark:zset-rem a
benchmark ZREMRANGEBYRANK benchmark:zset-rem-rank 0 0
benchmark ZREMRANGEBYSCORE benchmark:zset-rem-score 1 2
benchmark ZINTERSTORE benchmark:zinter-result 2 \
    benchmark:zset-a benchmark:zset-b
benchmark ZUNIONSTORE benchmark:zunion-result 2 \
    benchmark:zset-a benchmark:zset-b

# These are intentionally last because they remove the seeded dataset.
benchmark FLUSHDB
benchmark FLUSHALL

missing=()
for name in "${EXPECTED_COMMANDS[@]}"; do
    if [[ -z ${COVERED_COMMANDS[$name]:-} ]]; then
        missing+=("$name")
    fi
done

if (( ${#missing[@]} > 0 )); then
    echo "FAIL command coverage: missing ${missing[*]}"
    FAIL_COUNT=$((FAIL_COUNT + 1))
else
    echo "PASS command coverage (${#EXPECTED_COMMANDS[@]}/${#EXPECTED_COMMANDS[@]})"
    PASS_COUNT=$((PASS_COUNT + 1))
fi

echo
echo "Summary: $PASS_COUNT passed, $FAIL_COUNT failed"
{
    printf '\n# Throughput Summary\n\n'
    printf '| Command | Requests/sec | Result |\n'
    printf '|---|---:|---|\n'
    if ((${#SUMMARY_LINES[@]} > 0)); then
        printf '%s\n' "${SUMMARY_LINES[@]}"
    fi

    printf '\n# Summary\n\n'
    printf -- '- Commands passed: %s\n' "$PASS_COUNT"
    printf -- '- Commands failed: %s\n' "$FAIL_COUNT"
    printf -- '- Report path: %s\n' "$REPORT"
} >> "$REPORT"
if (( FAIL_COUNT > 0 )); then
    exit 1
fi
