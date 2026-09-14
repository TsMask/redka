#!/bin/bash
#
# Redka 全命令 redis-cli 功能测试脚本
# 用法: ./test_redis-cli.sh [port] [host] [password]
#
# 覆盖 server/internal/command/command.go 中支持的命令

set -e

PORT=${1:-6380}
HOST=${2:-127.0.0.1}
PASSWORD=${3:-}
CLI="redis-cli -h $HOST -p $PORT"
AUTH_REQUIRED=false
PASS_COUNT=0
FAIL_COUNT=0

# 如果配置了密码，使用 -a 参数（redis-cli 每次命令都会使用这个密码认证）
if [ -n "$PASSWORD" ]; then
    CLI="redis-cli -h $HOST -p $PORT -a $PASSWORD"
fi

# command.go 支持命令清单（小写）
SUPPORTED_COMMANDS=(
    command config dbsize flushdb flushall info lolwut
    echo ping select
    del exists expire expireat keys persist pexpire pexpireat randomkey rename renamenx scan ttl type
    lindex linsert llen lpop lpush lrange lrem lset ltrim rpop rpoplpush rpush
    append decr decrby get getrange getset incr incrby incrbyfloat mget mset psetex set setex setnx setrange strlen substr
    hdel hexists hget hgetall hincrby hincrbyfloat hkeys hlen hmget hmset hscan hset hsetnx hrandfield hstrlen hvals
    sadd scard sdiff sdiffstore sinter sintercard sinterstore sismember smembers smismember smove spop srandmember srem sscan sunion sunionstore
    zadd zcard zcount zincrby zinter zinterstore zrange zrangebyscore zrank zrem zremrangebyrank zremrangebyscore zrevrange zrevrangebyscore zrevrank zscan zscore zunion zunionstore
)

declare -A COVERED_COMMANDS=()

mark_covered() {
    local cmd="${1,,}"
    [ -n "$cmd" ] && COVERED_COMMANDS["$cmd"]=1
}

authenticate() {
    if [ -n "$PASSWORD" ]; then
        AUTH_REQUIRED=true
        echo "使用密码认证模式 (-a 参数)"
        # 验证密码是否正确（只检查最后一行是否为 OK）
        local result=$($CLI AUTH "$PASSWORD" 2>&1 | tail -1 | tr -d '\0')
        if [ "$result" = "OK" ]; then
            echo "AUTH 验证成功!"
        else
            echo "AUTH 验证失败: $result"
            echo "请检查密码是否正确"
            exit 1
        fi
    else
        echo "无需认证 (无密码配置)"
    fi
}

section() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

assert_eq() {
    local desc="$1" expect="$2"
    shift 2
    mark_covered "$1"
    local actual
    actual=$($CLI "$@" 2>/dev/null | tr -d '\0')
    if [ "$actual" = "$expect" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" "$desc"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected: '%s'\n" "$expect"
        printf "    Actual:   '%s'\n" "$actual"
    fi
}

assert_int() {
    local desc="$1" expect="$2"
    shift 2
    mark_covered "$1"
    local actual
    actual=$($CLI "$@" 2>/dev/null | tr -d '\0')
    if [ "$actual" = "(integer) $expect" ] || [ "$actual" = "$expect" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" "$desc"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected: '%s'\n" "$expect"
        printf "    Actual:   '%s'\n" "$actual"
    fi
}

assert_pattern() {
    local desc="$1" pattern="$2"
    shift 2
    mark_covered "$1"
    local actual
    actual=$($CLI "$@" 2>/dev/null | tr -d '\0')
    if echo "$actual" | grep -qE "$pattern" 2>/dev/null; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" "$desc"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected pattern: '%s'\n" "$pattern"
        printf "    Actual:   '%s'\n" "$actual"
    fi
}

assert_one_of() {
    local desc="$1" expected="$2"
    shift 2
    mark_covered "$1"
    local actual
    actual=$($CLI "$@" 2>/dev/null | tr -d '\0')
    local candidate
    local matched=false
    IFS='|' read -ra candidates <<< "$expected"
    for candidate in "${candidates[@]}"; do
        if [[ "$actual" == "$candidate" ]]; then
            matched=true
            break
        fi
    done
    if [[ "$matched" == true ]]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s (actual='%s')\n" "$desc" "$actual"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected one of: '%s'\n" "$expected"
        printf "    Actual:   '%s'\n" "$actual"
    fi
}

assert_unordered() {
    local desc="$1" expected="$2"
    shift 2
    mark_covered "$1"
    local actual normalized
    actual=$($CLI "$@" 2>/dev/null | tr -d '\0' | sort)
    normalized=$(printf '%s\n' "$expected" | sort)
    if [[ "$actual" == "$normalized" ]]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" "$desc"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected unordered lines:\n%s\n" "$normalized"
        printf "    Actual unordered lines:\n%s\n" "$actual"
    fi
}

assert_scan() {
    local desc="$1" expected_items="$2"
    shift 2
    mark_covered "$1"
    local raw cursor actual_items expected_sorted
    raw=$($CLI "$@" 2>/dev/null | tr -d '\0')
    cursor=$(printf '%s\n' "$raw" | head -n 1)
    actual_items=$(printf '%s\n' "$raw" | tail -n +2 | sort)
    expected_sorted=$(printf '%s\n' "$expected_items" | tr ' ' '\n' | sort)
    if [[ "$cursor" =~ ^[0-9]+$ && "$actual_items" == "$expected_sorted" ]]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s (cursor=%s)\n" "$desc" "$cursor"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected numeric cursor and items:\n%s\n" "$expected_sorted"
        printf "    Actual:\n%s\n" "$raw"
    fi
}

assert_ttl() {
    local desc="$1" min="$2" max="$3"
    shift 3
    mark_covered "$1"
    local raw val
    raw=$($CLI "$@" 2>/dev/null | tr -d '\0')
    val="${raw#* }"
    if [ "$val" -ge "$min" ] 2>/dev/null && [ "$val" -le "$max" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s (TTL=%s, range [%s,%s])\n" "$desc" "$val" "$min" "$max"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected TTL in [%s,%s], got: %s\n" "$min" "$max" "$raw"
    fi
}

assert_config_get_value() {
    local desc="$1" param="$2" expect="$3"
    COVERED_COMMANDS[config]=1
    local actual
    actual=$($CLI CONFIG GET "$param" 2>/dev/null | tail -n 1 | tr -d '\0')
    if [ "$actual" = "$expect" ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" "$desc"
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" "$desc"
        printf "    Expected: '%s'\n" "$expect"
        printf "    Actual:   '%s'\n" "$actual"
    fi
}

assert_coverage() {
    section "命令覆盖检查"
    local missing=()
    local cmd
    for cmd in "${SUPPORTED_COMMANDS[@]}"; do
        if [ -z "${COVERED_COMMANDS[$cmd]}" ]; then
            missing+=("$cmd")
        fi
    done

    if [ ${#missing[@]} -eq 0 ]; then
        PASS_COUNT=$((PASS_COUNT + 1))
        printf "  \033[32mPASS\033[0m %s\n" 覆盖了全部支持命令
    else
        FAIL_COUNT=$((FAIL_COUNT + 1))
        printf "  \033[31mFAIL\033[0m %s\n" " 存在未覆盖命令 (${#missing[@]}): ${missing[*]}"
    fi
}

echo "检查 Redka 连接 ($HOST:$PORT)..."
if ! $CLI PING >/dev/null 2>&1; then
    echo "错误: 无法连接到 Redka ($HOST:$PORT)"
    echo "请先启动 Redka 服务"
    exit 1
fi
echo "连接成功!"

# 认证检查
authenticate

$CLI FLUSHDB >/dev/null 2>&1 || true

section "1. 连接/服务器命令"
assert_eq  "PING"                        "PONG"              PING
assert_eq  "PING msg"                    "hello"             PING hello
assert_eq  "ECHO test"                   "test"              ECHO test
assert_eq  "SELECT 1"                    "OK"                SELECT 1
assert_eq  "SELECT 0"                    "OK"                SELECT 0
assert_int "DBSIZE (空库)"               "0"                 DBSIZE
assert_eq  "COMMAND"                     "OK"                COMMAND
assert_eq  "CONFIG GET"                                      $'requirepass\n********' CONFIG get requirepass
assert_eq  "CONFIG SET slowlog threshold"          "OK"              CONFIG SET slowlog-log-slower-than 250
assert_config_get_value "CONFIG GET slowlog threshold" "slowlog-log-slower-than" "250"
assert_eq  "CONFIG SET slowlog max length"         "OK"              CONFIG SET slowlog-max-len 128
assert_config_get_value "CONFIG GET slowlog max length" "slowlog-max-len" "128"
assert_pattern "CONFIG SET unsupported parameter"  "ERR Unsupported CONFIG parameter: requirepass" CONFIG SET requirepass secret
assert_eq  "CONFIG SET restore slowlog threshold"  "OK"              CONFIG SET slowlog-log-slower-than 1500000
assert_pattern "INFO contains server section"     "^# Server[[:space:]]?$" INFO
assert_pattern "INFO contains Redis version"      "^redis_version:[0-9]" INFO
assert_eq "LOLWUT"                               "Ask me a question (⊃｡•́‿•̀｡)⊃" LOLWUT
assert_eq  "FLUSHALL"                    "OK"                FLUSHALL

section "2. String 命令"
assert_eq  "SET str1 hello"              "OK"                SET str1 hello
assert_eq  "GET str1"                    "hello"             GET str1
assert_int "STRLEN str1"                 "5"                 STRLEN str1
assert_int "APPEND str1"                 "11"                APPEND str1 " world"
assert_eq  "GET str1 append后"           "hello world"       GET str1
assert_eq  "GETRANGE str1 0 4"           "hello"             GETRANGE str1 0 4
assert_eq  "SUBSTR str1 6 10"            "world"             SUBSTR str1 6 10

$CLI DEL nxkey >/dev/null 2>&1 || true
assert_int "SETNX nxkey (不存在)"        "1"                 SETNX nxkey newval
assert_eq  "GET nxkey"                   "newval"            GET nxkey
assert_int "SETNX str1 (已存在)"         "0"                 SETNX str1 updated

assert_eq  "SETEX str2"                  "OK"                SETEX str2 200 tmpval
assert_eq  "GET str2"                    "tmpval"            GET str2
assert_ttl "TTL str2"                    190 210             TTL str2

assert_eq  "PSETEX strps 3000"           "OK"                PSETEX strps 3000 psv
assert_eq  "GET strps"                   "psv"               GET strps
assert_ttl "TTL strps (毫秒转秒)"        1 5                 TTL strps

assert_eq  "SET str3 oldval"             "OK"                SET str3 oldval
assert_eq  "GETSET str3"                 "oldval"            GETSET str3 newval
assert_eq  "GET str3 更新"               "newval"            GET str3

assert_eq  "MSET"                        "OK"                MSET k1 v1 k2 v2 k3 v3
assert_eq "MGET"                          $'v1\nv2\nv3'       MGET k1 k2 k3

assert_eq  "SET counter"                 "OK"                SET counter 10
assert_int "INCR counter"                "11"                INCR counter
assert_int "INCRBY counter 5"            "16"                INCRBY counter 5
assert_int "DECR counter"                "15"                DECR counter
assert_int "DECRBY counter 2"            "13"                DECRBY counter 2
assert_eq  "INCRBYFLOAT counter 2.5"     "15.5"              INCRBYFLOAT counter 2.5

assert_int "SETRANGE srg 2 xyz"          "5"                 SETRANGE srg 2 xyz
assert_eq "GET srg"                       "xyz"               GET srg

section "3. Hash 命令"
assert_int "HSET user1 name"             "1"                 HSET user1 name Alice
assert_int "HSET user1 age"              "1"                 HSET user1 age 30
assert_int "HSET user1 email"            "1"                 HSET user1 email alice@test.com
assert_eq  "HGET user1 name"             "Alice"             HGET user1 name
assert_int "HLEN user1"                  "3"                 HLEN user1
assert_int "HEXISTS user1 name"          "1"                 HEXISTS user1 name
assert_unordered "HKEYS user1"           $'name\nage\nemail' HKEYS user1
assert_unordered "HVALS user1"           $'Alice\n30\nalice@test.com' HVALS user1
assert_int "HSETNX user1 name (已存在)"  "0"                 HSETNX user1 name Bob
assert_int "HSETNX user1 phone"          "1"                 HSETNX user1 phone 123456
assert_eq  "HMSET user2"                 "OK"                HMSET user2 name Charlie age 25 email charlie@test.com
assert_eq "HMGET user2"                   $'Charlie\n25\ncharlie@test.com' HMGET user2 name age email
assert_int "HDEL user2 email"            "1"                 HDEL user2 email
assert_unordered "HGETALL user2"         $'name\nCharlie\nage\n25' HGETALL user2
assert_int "HINCRBY user1 age 5"         "35"                HINCRBY user1 age 5
assert_eq  "HINCRBYFLOAT user1 age 0.5"  "35.5"              HINCRBYFLOAT user1 age 0.5
assert_int "HSTRLEN user1 name"          "5"                 HSTRLEN user1 name
assert_one_of "HRANDFIELD user1"          "name|age|email|phone" HRANDFIELD user1
assert_scan "HSCAN user1"                 "name Alice age 35.5 email alice@test.com phone 123456" HSCAN user1 0

section "4. List 命令"
assert_int "LPUSH mylist a"              "1"                 LPUSH mylist a
assert_int "LPUSH mylist b"              "2"                 LPUSH mylist b
assert_int "RPUSH mylist c"              "3"                 RPUSH mylist c
assert_int "RPUSH mylist d"              "4"                 RPUSH mylist d
assert_int "LLEN mylist"                 "4"                 LLEN mylist
assert_eq "LRANGE mylist 0 -1"            $'b\na\nc\nd'       LRANGE mylist 0 -1
assert_int "LPUSH multi a b c"           "3"                 LPUSH orderlist a b c
assert_eq "LPUSH multi order"             $'c\nb\na'          LRANGE orderlist 0 -1
assert_int "RPUSH multi x y"             "5"                 RPUSH orderlist x y
assert_eq "RPUSH multi order"             $'c\nb\na\nx\ny'    LRANGE orderlist 0 -1
assert_eq  "LINDEX mylist 0"             "b"                 LINDEX mylist 0
assert_eq  "LSET mylist 1"               "OK"                LSET mylist 1 updated
assert_int "LINSERT before c"            "5"                 LINSERT mylist before c inserted
assert_int "LREM mylist 1 xxx"           "0"                 LREM mylist 1 xxx
assert_eq  "LPOP mylist"                 "b"                 LPOP mylist
assert_eq  "RPOP mylist"                 "d"                 RPOP mylist
assert_int "RPUSH destlist"              "1"                 RPUSH destlist x
assert_eq  "RPOPLPUSH"                   "c"                 RPOPLPUSH mylist destlist
assert_int "RPUSH trimlist"              "6"                 RPUSH trimlist a b c d e f
assert_eq  "LTRIM"                       "OK"                LTRIM trimlist 1 3

section "5. Set 命令"
assert_int "SADD myset a"                "1"                 SADD myset a
assert_int "SADD myset b"                "1"                 SADD myset b
assert_int "SADD myset c"                "1"                 SADD myset c
assert_int "SADD myset d"                "1"                 SADD myset d
assert_int "SCARD myset"                 "4"                 SCARD myset
assert_int "SISMEMBER myset a"            "1"                 SISMEMBER myset a
assert_int "SISMEMBER myset z"            "0"                 SISMEMBER myset z
assert_int "SREM myset d"                "1"                 SREM myset d
assert_unordered "SMEMBERS myset"        $'a\nb\nc'          SMEMBERS myset

assert_int "SADD set1"                   "3"                 SADD set1 a b c
assert_int "SADD set2"                   "3"                 SADD set2 b c d
assert_unordered "SINTER"                $'b\nc'             SINTER set1 set2
assert_unordered "SUNION"                $'a\nb\nc\nd'       SUNION set1 set2
assert_unordered "SDIFF"                 "a"                 SDIFF set1 set2
assert_int "SINTERCARD"                  "2"                 SINTERCARD 2 set1 set2
assert_eq "SMISMEMBER"                    $'1\n0'            SMISMEMBER set1 a z

assert_int "SINTERSTORE rset"            "2"                 SINTERSTORE rset set1 set2
assert_int "SUNIONSTORE uset"            "4"                 SUNIONSTORE uset set1 set2
assert_int "SDIFFSTORE dset"             "1"                 SDIFFSTORE dset set1 set2
assert_int "SADD movetest"               "3"                 SADD movetest x y z
assert_int "SMOVE x -> myset"            "1"                 SMOVE movetest myset x
assert_int "SADD single-pop set"          "1"                 SADD single-pop a
assert_eq  "SPOP single-pop set"          "a"                 SPOP single-pop
assert_int "SCARD single-pop set"         "0"                 SCARD single-pop
assert_one_of "SRANDMEMBER myset"         "a|b|c|x"          SRANDMEMBER myset
assert_scan "SSCAN myset"                 "a b c x"          SSCAN myset 0

section "6. Sorted Set 命令"
assert_int "ZADD zset1 1 one"            "1"                 ZADD zset1 1 one
assert_int "ZADD zset1 2 two"            "1"                 ZADD zset1 2 two
assert_int "ZADD zset1 3 three"          "1"                 ZADD zset1 3 three
assert_int "ZADD zset1 4 four"           "1"                 ZADD zset1 4 four
assert_int "ZADD zset1 5 five"           "1"                 ZADD zset1 5 five
assert_int "ZCARD zset1"                 "5"                 ZCARD zset1
assert_eq  "ZSCORE one"                  "1"                 ZSCORE zset1 one
assert_int "ZRANK three"                 "2"                 ZRANK zset1 three
assert_int "ZREVRANK one"                "4"                 ZREVRANK zset1 one
assert_eq "ZRANGE 0 2"                    $'one\ntwo\nthree' ZRANGE zset1 0 2
assert_eq "ZREVRANGE 0 2"                 $'five\nfour\nthree' ZREVRANGE zset1 0 2
assert_eq "ZRANGEBYSCORE 2 4"             $'two\nthree\nfour' ZRANGEBYSCORE zset1 2 4
assert_eq "ZREVRANGEBYSCORE 4 2"          $'four\nthree\ntwo' ZREVRANGEBYSCORE zset1 4 2
assert_int "ZCOUNT 2 4"                  "3"                 ZCOUNT zset1 2 4
assert_eq  "ZINCRBY one +10"             "11"                ZINCRBY zset1 10 one
assert_int "ZREM one"                    "1"                 ZREM zset1 one

assert_int "ZADD zset2"                  "3"                 ZADD zset2 1 a 2 b 3 c
assert_int "ZADD zset3"                  "3"                 ZADD zset3 2 b 3 c 4 d
assert_unordered "ZINTER"                $'b\nc'             ZINTER 2 zset2 zset3
assert_unordered "ZUNION"                $'a\nb\nc\nd'       ZUNION 2 zset2 zset3
assert_int "ZINTERSTORE zresult"         "2"                 ZINTERSTORE zresult 2 zset2 zset3
assert_int "ZUNIONSTORE zuresult"        "4"                 ZUNIONSTORE zuresult 2 zset2 zset3
assert_int "ZADD remtest"                "4"                 ZADD remtest 1 a 2 b 3 c 4 d
assert_int "ZREMRANGEBYRANK 0 0"         "1"                 ZREMRANGEBYRANK remtest 0 0
assert_int "ZREMRANGEBYSCORE 2 3"         "2"                ZREMRANGEBYSCORE remtest 2 3
assert_scan "ZSCAN zset1"                 "two 2 three 3 four 4 five 5" ZSCAN zset1 0

section "7. Key 命令"
assert_eq  "SET mykey"                   "OK"                SET mykey hello
assert_eq  "TYPE mykey"                  "string"            TYPE mykey
assert_int "EXISTS mykey"                "1"                 EXISTS mykey
assert_int "EXISTS nonexist"             "0"                 EXISTS nonexist
assert_int "HSET hkey"                   "1"                 HSET hkey f1 v1
assert_eq "KEYS exact key"                "mykey"            KEYS "mykey"

assert_eq  "SET renamekey val"           "OK"                SET renamekey val
assert_eq  "RENAME renamekey"            "OK"                RENAME renamekey newname
assert_int "RENAMENX -> mykey (已存在)"  "0"                 RENAMENX newname mykey
assert_eq  "SET srcnew val2"             "OK"                SET srcnew val2
assert_int "RENAMENX -> dstnew"          "1"                 RENAMENX srcnew dstnew

assert_int "DEL mykey"                   "1"                 DEL mykey
assert_int "EXISTS mykey 已删除"         "0"                 EXISTS mykey

assert_eq  "SET expkey"                  "OK"                SET expkey val
assert_int "EXPIRE expkey 100"           "1"                 EXPIRE expkey 100
assert_ttl "TTL expkey"                  90 115              TTL expkey
assert_int "PERSIST expkey"              "1"                 PERSIST expkey
assert_int "TTL expkey after PERSIST"    "-1"                TTL expkey

assert_int "EXPIREAT expkey"             "1"                 EXPIREAT expkey 9999999999
assert_ttl "TTL expkey after EXPIREAT"   1000000000 9999999999 TTL expkey
assert_eq  "SET pexp pval"               "OK"                SET pexp pval
assert_int "PEXPIRE pexp 3000"           "1"                 PEXPIRE pexp 3000
assert_ttl "TTL pexp"                    1 5                 TTL pexp
assert_eq  "SET pexpat pval"             "OK"                SET pexpat pval
assert_int "PEXPIREAT pexpat 4102444800000" "1"              PEXPIREAT pexpat 4102444800000
assert_ttl "TTL pexpat"                  1000000000 9999999999 TTL pexpat

assert_eq "FLUSHDB before RANDOMKEY"     "OK"                FLUSHDB
assert_eq  "SET randkey1"                "OK"                SET randkey1 a
assert_eq  "SET randkey2"                "OK"                SET randkey2 b
assert_one_of "RANDOMKEY"                "randkey1|randkey2" RANDOMKEY
assert_scan "SCAN benchmark keys"        "randkey1 randkey2" SCAN 0 MATCH randkey* COUNT 100
assert_eq  "FLUSHDB"                     "OK"                FLUSHDB
assert_int "DBSIZE 最终"                 "0"                 DBSIZE

assert_coverage

TOTAL=$((PASS_COUNT + FAIL_COUNT))
section "测试结果汇总"
echo "通过: $PASS_COUNT"
echo "失败: $FAIL_COUNT"
echo "总计: $TOTAL"
if [ $FAIL_COUNT -gt 0 ]; then
    RATE=$(awk "BEGIN{printf \"%.1f\", $PASS_COUNT*100/$TOTAL}")
    echo "通过率: ${RATE}%"
    echo "存在失败的测试用例!"
    exit 1
else
    echo "通过率: 100.0%"
    echo "所有测试用例通过!"
    exit 0
fi
