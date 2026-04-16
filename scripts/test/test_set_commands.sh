#!/bin/bash

# Redis SET命令完整测试脚本
# 用法: ./test_set_commands.sh [port] [host] [password]
# 默认: localhost 6379 (无密码)

PORT=${1:-6379}
HOST=${2:-localhost}
PASS=${3:-}

# 构建redis-cli命令（使用--no-auth-warning避免Warning）
if [ -z "$PASS" ]; then
    REDIS_CLI="redis-cli -h $HOST -p $PORT"
else
    REDIS_CLI="redis-cli -h $HOST -p $PORT -a $PASS --no-auth-warning"
fi

echo "=========================================="
echo "Redis SET 命令完整测试"
echo "目标: $HOST:$PORT"
if [ -n "$PASS" ]; then
    echo "密码: ********"
fi
echo "=========================================="
echo ""

# 测试辅助函数
test_command() {
    local description=$1
    local command=$2
    local expected=$3
    local use_echo=false
    local use_wildcard=false
    
    # Check if expected value uses -e flag
    if [[ "$expected" == "-e" ]]; then
        use_echo=true
        shift 3
        expected="$1"
    fi
    
    # Check if expected value uses wildcard matching
    if [[ "$expected" == *"*"* ]]; then
        use_wildcard=true
    fi
    
    echo "----------------------------------------"
    echo "测试: $description"
    echo "命令: $command"
    
    if [[ "$use_echo" == "true" ]]; then
        # Use echo -e to interpret escape sequences
        expected=$(echo -e "$expected")
    fi
    
    result=$(echo "$command" | $REDIS_CLI 2>&1)
    echo "结果: $result"
    echo "期望: $expected"
    
    local passed=false
    if [[ "$result" == "$expected" ]]; then
        passed=true
    elif [[ "$use_wildcard" == "true" ]]; then
        # Convert wildcard pattern to regex
        local pattern="${expected//\*/.*}"
        if [[ "$result" =~ $pattern ]]; then
            passed=true
        fi
    fi
    
    if [[ "$passed" == "true" ]]; then
        echo "✅ 通过"
    else
        echo "❌ 失败"
    fi
    echo ""
}

# 清理函数
cleanup() {
    echo "清理测试数据..."
    $REDIS_CLI DEL key1 key2 key3 key4 key5 key6 key7 key8 > /dev/null 2>&1
    echo "清理完成"
    echo ""
}

# 开始测试
cleanup

echo "=========================================="
echo "1. 基本SET操作测试"
echo "=========================================="
echo ""

test_command "基本SET - 设置字符串值" "SET key1 hello" "OK"
test_command "基本GET - 获取字符串值" "GET key1" "hello"
test_command "SET覆盖值" "SET key1 world" "OK"
test_command "验证覆盖" "GET key1" "world"

echo "=========================================="
echo "2. NX选项测试 (SET key value NX)"
echo "=========================================="
echo ""

test_command "NX - key不存在时设置 (应返回OK)" "SET key2 value2 NX" "OK"
test_command "NX - key已存在时设置 (应返回nil)" "SET key2 newvalue2 NX" ""
test_command "验证key2未被覆盖" "GET key2" "value2"
test_command "NX - 对不存在的key设置 (应返回OK)" "SET key3 value3 NX" "OK"

echo "=========================================="
echo "3. XX选项测试 (SET key value XX)"
echo "=========================================="
echo ""

test_command "XX - key已存在时设置 (应返回OK)" "SET key1 newworld XX" "OK"
test_command "验证key1被更新" "GET key1" "newworld"
test_command "XX - key不存在时设置 (应返回nil)" "SET nonexistent value XX" ""
test_command "验证nonexistent不存在" "GET nonexistent" ""

echo "=========================================="
echo "4. GET选项测试 (SET key value GET)"
echo "=========================================="
echo ""

test_command "GET - key已存在，返回旧值" "SET key1 hello GET" "newworld"
test_command "验证key1被更新" "GET key1" "hello"
test_command "GET - key不存在，返回nil" "SET key4 value4 GET" ""

echo "=========================================="
echo "5. EX选项测试 (SET key value EX seconds)"
echo "=========================================="
echo ""

test_command "EX - 设置带过期时间(2秒)" "SET key5 expirevalue EX 2" "OK"
test_command "验证TTL存在" "TTL key5" "2"
test_command "验证值正确" "GET key5" "expirevalue"
echo "等待3秒让key过期..."
sleep 3
test_command "验证key已过期" "GET key5" ""

echo "=========================================="
echo "6. PX选项测试 (SET key value PX milliseconds)"
echo "=========================================="
echo ""

test_command "PX - 设置带过期时间(2000毫秒)" "SET key6 pxtest PX 2000" "OK"
test_command "验证TTL存在 (约2秒)" "TTL key6" "2"
test_command "验证值正确" "GET key6" "pxtest"

echo "=========================================="
echo "7. EXAT选项测试 (SET key value EXAT timestamp)"
echo "=========================================="
echo ""

FUTURE_TIME=$(date -d "+5 seconds" +%s)
test_command "EXAT - 设置未来时间戳" "SET key7 exattest EXAT $FUTURE_TIME" "OK"
test_command "验证TTL约5秒" "TTL key7" "5"
test_command "验证值正确" "GET key7" "exattest"

echo "=========================================="
echo "8. PXAT选项测试 (SET key value PXAT timestamp)"
echo "=========================================="
echo ""

FUTURE_MS=$(date -d "+5 seconds" +%s%3N)
test_command "PXAT - 设置未来毫秒时间戳" "SET key8 pxattext PXAT $FUTURE_MS" "OK"
test_command "验证TTL约5秒" "TTL key8" "5"
test_command "验证值正确" "GET key8" "pxattext"

echo "=========================================="
echo "9. KEEPTTL选项测试 (SET key value KEEPTTL)"
echo "=========================================="
echo ""

test_command "先设置带TTL的key" "SET key1 initialvalue EX 100" "OK"
test_command "验证TTL" "TTL key1" "100"
sleep 1
test_command "KEEPTTL - 保持TTL更新值" "SET key1 newvalue KEEPTTL" "OK"
test_command "验证TTL保持 (约99秒)" "TTL key1" "99"
test_command "验证值被更新" "GET key1" "newvalue"

echo "=========================================="
echo "10. NX + GET组合测试 (Redis 7.0+)"
echo "=========================================="
echo ""

$REDIS_CLI DEL testnx > /dev/null 2>&1
test_command "NX+GET - key不存在，返回nil" "SET testnx value NX GET" ""
test_command "NX+GET - key已存在，返回旧值" "SET testnx newvalue NX GET" "value"
test_command "验证值未改变" "GET testnx" "value"

echo "=========================================="
echo "11. XX + GET组合测试 (Redis 7.0+)"
echo "=========================================="
echo ""

$REDIS_CLI DEL testxx > /dev/null 2>&1
test_command "XX+GET - key不存在，返回nil" "SET testxx value XX GET" ""
test_command "先设置key" "SET testxx initial" "OK"
test_command "XX+GET - key已存在，返回旧值" "SET testxx newvalue XX GET" "initial"
test_command "验证值被更新" "GET testxx" "newvalue"

echo "=========================================="
echo "12. 组合选项测试"
echo "=========================================="
echo ""

test_command "NX + EX组合 - 不存在的key" "SET combo1 val NX EX 10" "OK"
test_command "验证TTL" "TTL combo1" "10"
test_command "NX + EX组合 - 已存在的key" "SET combo1 val2 NX EX 10" ""

test_command "XX + EX组合 - 已存在的key" "SET combo1 val3 XX EX 20" "OK"
test_command "验证TTL更新" "TTL combo1" "20"
test_command "XX + EX组合 - 不存在的key" "SET newcombo val XX EX 10" ""

echo "=========================================="
echo "13. 边界条件测试"
echo "=========================================="
echo ""

test_command "空值设置" "SET emptykey \"\"" "OK"
test_command "获取空值" "GET emptykey" ""
test_command "特殊字符值" "SET specialkey \"hello\\nworld\\n\"" "OK"
test_command "获取特殊字符值" "GET specialkey" -e "hello\nworld\n"
test_command "大数值" "SET bigkey 12345678901234567890" "OK"
test_command "获取大数值" "GET bigkey" "12345678901234567890"

echo "=========================================="
echo "14. 类型检查测试"
echo "=========================================="
echo ""

$REDIS_CLI DEL hashkey listkey setkey > /dev/null 2>&1
$REDIS_CLI HSET hashkey field value > /dev/null 2>&1
$REDIS_CLI LPUSH listkey item > /dev/null 2>&1
$REDIS_CLI SADD setkey member > /dev/null 2>&1

echo "已创建非字符串类型key (hash, list, set)"
test_command "对Hash类型使用SET (应报错)" "SET hashkey newvalue" "WRONGTYPE*"
test_command "对List类型使用SET (应报错)" "SET listkey newvalue" "WRONGTYPE*"
test_command "对Set类型使用SET (应报错)" "SET setkey newvalue" "WRONGTYPE*"

echo "=========================================="
echo "15. 原子性测试"
echo "=========================================="
echo ""

$REDIS_CLI DEL atomtest > /dev/null 2>&1
test_command "SET + GET原子操作" "SET atomtest value GET" ""
test_command "验证值设置" "GET atomtest" "value"
test_command "SET + GET在已存在的key上" "SET atomtest newvalue GET" "value"
test_command "验证值更新" "GET atomtest" "newvalue"

echo "=========================================="
echo "测试完成"
echo "=========================================="
echo ""
echo "清理测试数据..."
cleanup
echo ""
echo "所有测试完成！"
