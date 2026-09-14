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

CLI=(redis-cli -h "$HOST" -p "$PORT" -a "$PASSWORD" --no-auth-warning --raw)
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
SEMANTIC_PASS_COUNT=0
SEMANTIC_FAIL_COUNT=0
CONCURRENT_TMP=""

die() {
    echo "ERROR: $*" >&2
    exit 2
}

if [[ ! $REQUESTS =~ ^[0-9]+$ || $REQUESTS -lt 10 ]]; then
    die "requests must be an integer of at least 10"
fi
if [[ ! $CLIENTS =~ ^[0-9]+$ || $CLIENTS -lt 1 ]]; then
    die "clients must be a positive integer"
fi

cleanup() {
    if [[ -n $CONCURRENT_TMP ]]; then
        rm -rf "$CONCURRENT_TMP"
    fi
}
trap cleanup EXIT

record_semantic() {
    local description=$1 expected=$2 actual=$3
    if [[ "$expected" == "$actual" ]]; then
        SEMANTIC_PASS_COUNT=$((SEMANTIC_PASS_COUNT + 1))
        echo "PASS semantic: $description"
    else
        SEMANTIC_FAIL_COUNT=$((SEMANTIC_FAIL_COUNT + 1))
        echo "FAIL semantic: $description (expected='$expected', actual='$actual')"
    fi
}

raw() {
    local output
    if ! output=$("${CLI[@]}" "$@" 2>/dev/null); then
        printf ''
        return
    fi
    printf '%s\n' "$output"
}

semantic_value() {
    local description=$1 expected=$2
    shift 2
    local actual
    actual=$(raw "$@" | tr -d '\0')
    record_semantic "$description" "$expected" "$actual"
}

semantic_int() {
    local description=$1 expected=$2
    shift 2
    local actual
    actual=$(raw "$@" | tr -d '\0')
    actual=${actual#\(*integer\)}
    record_semantic "$description" "$expected" "$actual"
}

semantic_unordered() {
    local description=$1 expected=$2
    shift 2
    local actual normalized
    actual=$(raw "$@" | tr -d '\0' | sort)
    normalized=$(printf '%s\n' "$expected" | sort)
    record_semantic "$description" "$normalized" "$actual"
}

assert_scan_semantics() {
    local description=$1 expected_items=$2
    shift 2
    local raw_output cursor actual expected
    raw_output=$(raw "$@" | tr -d '\0')
    cursor=$(printf '%s\n' "$raw_output" | head -n 1)
    actual=$(printf '%s\n' "$raw_output" | tail -n +2 | sort)
    expected=$(printf '%s\n' "$expected_items" | tr ' ' '\n' | sort)
    if [[ "$cursor" =~ ^[0-9]+$ && "$actual" == "$expected" ]]; then
        SEMANTIC_PASS_COUNT=$((SEMANTIC_PASS_COUNT + 1))
        echo "PASS semantic: $description"
    else
        SEMANTIC_FAIL_COUNT=$((SEMANTIC_FAIL_COUNT + 1))
        echo "FAIL semantic: $description (expected cursor=numeric items='$expected', actual='$raw_output')"
    fi
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
    if [[ $status == pass && ( -z $rps || ( ! $rps =~ ^[0-9]+([.][0-9]+)?$ && $rps != inf ) ) ]]; then
        FAIL_COUNT=$((FAIL_COUNT + 1))
        status=fail
        echo "FAIL $name (missing or invalid requests-per-second measurement)"
    fi
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

check_semantics() {
    local counter_expected float_expected hash_age_expected lpop_expected
    counter_expected=$((10 + REQUESTS * 3))
    float_expected=$(awk -v n="$REQUESTS" 'BEGIN {printf "%.17g", 1.5 + n * 2.5}')
    hash_age_expected=$(awk -v n="$REQUESTS" 'BEGIN {printf "%.17g", 30 + n * 1.5}')
    lpop_expected=$(seq "$((REQUESTS - 2))" "$((REQUESTS + 3))")

    section_header="Semantic correctness"
    echo
    echo "$section_header"

    # String state and arithmetic invariants.
    semantic_value "SAVE returns explicit success reply" "OK" SAVE
    semantic_value "SAVE preserves seeded data" "hello world" GET benchmark:string
    semantic_value "GETSET final value" "new value" GET benchmark:getset
    semantic_value "SET final value" "new value" GET benchmark:string-write
    semantic_value "SETEX final value" "value" GET benchmark:setex
    semantic_value "PSETEX final value" "value" GET benchmark:psetex
    semantic_value "SETNX creates once" "value" GET benchmark:setnx
    semantic_int "SETRANGE final length" "11" STRLEN benchmark:setrange
    semantic_value "MSET first value" "a" GET benchmark:mset-a
    semantic_value "MSET second value" "b" GET benchmark:mset-b
    semantic_int "APPEND accumulates every write" "$((5 + REQUESTS * 6))" STRLEN benchmark:append
    semantic_int "INCR/INCRBY/DECR/DECRBY counter" "$counter_expected" GET benchmark:counter
    semantic_value "INCRBYFLOAT counter" "$float_expected" GET benchmark:float-counter

    # Hash field and arithmetic invariants.
    semantic_value "HSET final value" "China" HGET benchmark:hash country
    semantic_value "HSETNX creates once" "Go" HGET benchmark:hash language
    semantic_value "HMSET final value" "engineer" HGET benchmark:hash role
    semantic_value "HINCRBYFLOAT field" "$hash_age_expected" HGET benchmark:hash age
    semantic_unordered "HDEL removes field" $'name\nage\ncity\ncountry\nlanguage\nrole' HKEYS benchmark:hash
    semantic_int "HLEN final field count" "6" HLEN benchmark:hash

    # List length and content invariants.
    semantic_value "LSET updates target" $'x\nupdated\nz' LRANGE benchmark:lset 0 -1
    semantic_int "LINSERT inserts every request" "$((REQUESTS + 3))" LLEN benchmark:linsert
    semantic_value "LREM removes all matching elements" $'b\nc' LRANGE benchmark:lrem 0 -1
    semantic_value "LPOP leaves final six elements" "$lpop_expected" LRANGE benchmark:lpop 0 -1
    semantic_value "RPOP leaves first six elements" $'a\nb\nc\n1\n2\n3' LRANGE benchmark:rpop 0 -1
    semantic_int "RPOPLPUSH source length" "6" LLEN benchmark:rpoplpush-src
    semantic_int "RPOPLPUSH destination length" "$((REQUESTS + 1))" LLEN benchmark:rpoplpush-dst
    semantic_value "LTRIM keeps requested range" $'a\nb' LRANGE benchmark:ltrim 0 -1
    semantic_int "LPUSH appends every request" "$REQUESTS" LLEN benchmark:lpush-write
    semantic_int "RPUSH appends every request" "$REQUESTS" LLEN benchmark:rpush-write

    # Set membership and cardinality invariants.
    semantic_unordered "SADD accumulates distinct members" $'another\nx' SMEMBERS benchmark:set-write
    semantic_int "SPOP leaves final six members" "6" SCARD benchmark:spop
    semantic_unordered "SREM removes existing member only" $'y\nz' SMEMBERS benchmark:srem
    semantic_unordered "SMOVE moves existing member only" $'y\nz' SMEMBERS benchmark:smove-src
    semantic_unordered "SINTERSTORE result" "b" SMEMBERS benchmark:sinter-result
    semantic_unordered "SUNIONSTORE result" $'a\nb\nc\nd' SMEMBERS benchmark:sunion-result
    semantic_unordered "SDIFFSTORE result" "a" SMEMBERS benchmark:sdiff-result

    # Sorted-set score and membership invariants.
    semantic_value "ZADD is idempotent" "2" ZSCORE benchmark:zset-write another
    semantic_value "ZINCRBY accumulates every request" "$((1 + REQUESTS * 2))" ZSCORE benchmark:zset-incr member
    semantic_value "ZREM removes existing member only" "b" ZRANGE benchmark:zset-rem 0 -1
    semantic_value "ZREMRANGEBYRANK eventually removes all members" "" ZRANGE benchmark:zset-rem-rank 0 -1
    semantic_value "ZREMRANGEBYSCORE removes matching range once" "c" ZRANGE benchmark:zset-rem-score 0 -1
    semantic_value "ZINTERSTORE result" $'b\nc' ZRANGE benchmark:zinter-result 0 -1
    semantic_value "ZUNIONSTORE result" $'a\nb\nd\nc' ZRANGE benchmark:zunion-result 0 -1

    # Scan replies must have a numeric cursor and exact element set.
    assert_scan_semantics "HSCAN final fields" \
        "name Alice age $hash_age_expected city Beijing country China language Go role engineer" \
        HSCAN benchmark:hash 0 COUNT 100
    assert_scan_semantics "SSCAN final members" "a b c d" SSCAN benchmark:set 0 COUNT 100
    assert_scan_semantics "ZSCAN final members" "one 1 two 2 three 3 four 4 five 5" ZSCAN benchmark:zset 0 COUNT 100
}

run_concurrent_invariants() {
    local workers="$CLIENTS"
    local per_worker=$((REQUESTS / CLIENTS))
    if (( per_worker < 5 )); then
        per_worker=5
    elif (( per_worker > 20 )); then
        per_worker=20
    fi
    local total_operations=$((workers * per_worker))
    local expected_increment_sum=$((total_operations * (total_operations + 1) / 2))

    CONCURRENT_TMP=$(mktemp -d)
    run DEL benchmark:concurrent:list benchmark:concurrent:counter
    run SET benchmark:concurrent:counter 0

    worker() {
        local id=$1 increment_sum=0 push_replies=0 reply
        local i
        for ((i = 0; i < per_worker; i++)); do
            reply=$("${CLI[@]}" INCR benchmark:concurrent:counter) || return 1
            [[ $reply =~ ^[0-9]+$ ]] || return 1
            increment_sum=$((increment_sum + reply))

            reply=$("${CLI[@]}" LPUSH benchmark:concurrent:list "worker-${id}-${i}") || return 1
            [[ $reply =~ ^[0-9]+$ ]] || return 1
            push_replies=$((push_replies + 1))
        done
        printf '%s\n%s\n' "$increment_sum" "$push_replies"
    }

    local pids=()
    local worker_id
    for ((worker_id = 0; worker_id < workers; worker_id++)); do
        worker "$worker_id" >"$CONCURRENT_TMP/worker-$worker_id" &
        pids+=("$!")
    done

    local pid failed=false
    for pid in "${pids[@]}"; do
        if ! wait "$pid"; then
            failed=true
        fi
    done
    if [[ $failed == true ]]; then
        SEMANTIC_FAIL_COUNT=$((SEMANTIC_FAIL_COUNT + 1))
        echo "FAIL semantic: concurrent workers returned errors"
        return
    fi

    local increment_sum=0 push_replies=0 file
    for file in "$CONCURRENT_TMP"/worker-*; do
        mapfile -t values <"$file"
        increment_sum=$((increment_sum + values[0]))
        push_replies=$((push_replies + values[1]))
    done

    local final_counter final_list_len
    final_counter=$(raw GET benchmark:concurrent:counter)
    final_list_len=$(raw LLEN benchmark:concurrent:list)
    record_semantic "concurrent INCR returns every value once" \
        "$expected_increment_sum" "$increment_sum"
    record_semantic "concurrent INCR final value" "$total_operations" "$final_counter"
    local actual_invariant="$total_operations:$push_replies:$final_list_len"
    record_semantic "concurrent LPUSH reply count equals final LLEN" \
        "$total_operations:$total_operations:$total_operations" "$actual_invariant"
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
benchmark LTRIM benchmark:ltrim 0 1
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

check_semantics

run_concurrent_invariants

# These are intentionally last because they remove the seeded dataset.
benchmark FLUSHDB
benchmark FLUSHALL

semantic_int "FLUSHALL removes all keys" "0" DBSIZE

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
echo "Benchmark summary: $PASS_COUNT passed, $FAIL_COUNT failed"
echo "Semantic summary: $SEMANTIC_PASS_COUNT passed, $SEMANTIC_FAIL_COUNT failed"
FAIL_COUNT=$((FAIL_COUNT + SEMANTIC_FAIL_COUNT))
PASS_COUNT=$((PASS_COUNT + SEMANTIC_PASS_COUNT))
echo "Overall summary: $PASS_COUNT passed, $FAIL_COUNT failed"
{
    printf '\n# Throughput Summary\n\n'
    printf '| Command | Requests/sec | Result |\n'
    printf '|---|---:|---|\n'
    if ((${#SUMMARY_LINES[@]} > 0)); then
        printf '%s\n' "${SUMMARY_LINES[@]}"
    fi

    printf '\n# Summary\n\n'
    printf -- '- Benchmark checks passed: %s\n' "$((PASS_COUNT - SEMANTIC_PASS_COUNT))"
    printf -- '- Benchmark checks failed: %s\n' "$((FAIL_COUNT - SEMANTIC_FAIL_COUNT))"
    printf -- '- Semantic checks passed: %s\n' "$SEMANTIC_PASS_COUNT"
    printf -- '- Semantic checks failed: %s\n' "$SEMANTIC_FAIL_COUNT"
    printf -- '- Commands passed: %s\n' "$PASS_COUNT"
    printf -- '- Commands failed: %s\n' "$FAIL_COUNT"
    printf -- '- Report path: %s\n' "$REPORT"
} >> "$REPORT"
if (( FAIL_COUNT > 0 )); then
    exit 1
fi
