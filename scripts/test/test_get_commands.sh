#!/bin/bash

# Redis GET命令完整测试脚本
# 用法: ./test_get_commands.sh [port] [host] [password]
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
echo "Redis GET 命令完整测试"
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
    $REDIS_CLI DEL get1 get2 get3 get4 get5 get6 get7 get8 nonexist emptykey bigkey hashkey listkey setkey > /dev/null 2>&1
    echo "清理完成"
    echo ""
}

# 开始测试
cleanup

echo "=========================================="
echo "1. 基本GET操作测试"
echo "=========================================="
echo ""

test_command "基本GET - 获取字符串值" "SET get1 hello" "OK"
test_command "基本GET - 获取已存在的值" "GET get1" "hello"
test_command "GET - 修改值" "SET get1 world" "OK"
test_command "GET - 验证修改" "GET get1" "world"

echo "=========================================="
echo "2. GET不存在的Key"
echo "=========================================="
echo ""

test_command "GET - 不存在的key返回nil" "GET nonexist" ""
test_command "GET - 空值的key" "SET emptykey \"\"" "OK"
test_command "GET - 获取空值" "GET emptykey" ""

echo "=========================================="
echo "3. GET过期Key"
echo "=========================================="
echo ""

test_command "设置带过期时间的key" "SET get2 expirevalue EX 1" "OK"
test_command "GET - 未过期key" "GET get2" "expirevalue"
echo "等待2秒让key过期..."
sleep 2
test_command "GET - 已过期的key返回nil" "GET get2" ""

echo "=========================================="
echo "4. GET非字符串类型Key"
echo "=========================================="
echo ""

$REDIS_CLI DEL hashkey listkey setkey > /dev/null 2>&1
test_command "创建Hash类型" "HSET hashkey field value" "1"
test_command "GET - Hash类型应报错" "GET hashkey" "WRONGTYPE*"

test_command "创建List类型" "LPUSH listkey item" "1"
test_command "GET - List类型应报错" "GET listkey" "WRONGTYPE*"

test_command "创建Set类型" "SADD setkey member" "1"
test_command "GET - Set类型应报错" "GET setkey" "WRONGTYPE*"

echo "=========================================="
echo "5. GET批量操作"
echo "=========================================="
echo ""

test_command "设置多个key" "SET get3 val3" "OK"
test_command "设置多个key" "SET get4 val4" "OK"
test_command "设置多个key" "SET get5 val5" "OK"

echo "----------------------------------------"
echo "测试: MGET - 批量获取多个key"
echo "命令: MGET get1 get2 get3 get4 get5"
result=$(echo "MGET get1 get2 get3 get4 get5" | $REDIS_CLI 2>&1)
echo "结果: $result"
# MGET返回多行结果，检查是否包含所有值
if [[ "$result" == *"world"* ]] && [[ "$result" == *"val3"* ]] && [[ "$result" == *"val4"* ]] && [[ "$result" == *"val5"* ]]; then
    echo "期望: 包含world, val3, val4, val5"
    echo "✅ 通过"
else
    echo "期望: 包含world, val3, val4, val5"
    echo "❌ 失败"
fi
echo ""

echo "----------------------------------------"
echo "测试: MGET - 包含不存在的key"
echo "命令: MGET get1 nonexist get3"
result=$(echo "MGET get1 nonexist get3" | $REDIS_CLI 2>&1)
echo "结果: $result"
if [[ "$result" == *"world"* ]] && [[ "$result" == *"val3"* ]]; then
    echo "期望: 包含world和val3，nonexist返回空"
    echo "✅ 通过"
else
    echo "期望: 包含world和val3，nonexist返回空"
    echo "❌ 失败"
fi
echo ""

echo "=========================================="
echo "6. GET特殊值测试"
echo "=========================================="
echo ""

test_command "GET - 大数值" "SET bigkey 12345678901234567890" "OK"
test_command "GET - 获取大数值" "GET bigkey" "12345678901234567890"

test_command "GET - 特殊字符" "SET get6 \"hello\\nworld\\n\"" "OK"
test_command "GET - 获取特殊字符" "GET get6" -e "hello\nworld\n"

test_command "GET - 空格值" "SET get7 \"  value  \"" "OK"
test_command "GET - 获取空格值" "GET get7" "  value  "

test_command "GET - 数字值" "SET get8 42" "OK"
test_command "GET - 获取数字值" "GET get8" "42"

echo "=========================================="
echo "7. GETRANGE命令测试"
echo "=========================================="
echo ""

test_command "设置字符串" "SET rangekey \"Hello World\"" "OK"
test_command "GETRANGE - 获取全部" "GETRANGE rangekey 0 -1" "Hello World"
test_command "GETRANGE - 获取前5个字符" "GETRANGE rangekey 0 4" "Hello"
test_command "GETRANGE - 获取后5个字符" "GETRANGE rangekey -5 -1" "World"
test_command "GETRANGE - 获取中间字符" "GETRANGE rangekey 6 10" "World"
test_command "GETRANGE - 越界起始位置" "GETRANGE rangekey 100 200" ""

echo "=========================================="
echo "8. TTL和GET组合测试"
echo "=========================================="
echo ""

test_command "设置带TTL的key" "SET ttlkey value EX 60" "OK"
test_command "TTL - 获取剩余时间" "TTL ttlkey" "60"
test_command "GET - TTL key的值" "GET ttlkey" "value"
echo "等待1秒..."
sleep 1
test_command "TTL - 验证TTL减少" "TTL ttlkey" "59"

echo "=========================================="
echo "9. GET操作的原子性"
echo "=========================================="
echo ""

test_command "SET一个值" "SET atomkey original" "OK"
test_command "GET - 验证值" "GET atomkey" "original"
test_command "SET - 覆盖值" "SET atomkey newvalue" "OK"
test_command "GET - 验证新值" "GET atomkey" "newvalue"

echo "=========================================="
echo "10. GET与类型转换"
echo "=========================================="
echo ""

test_command "SET整数值" "SET intkey 100" "OK"
test_command "GET - 获取整数值" "GET intkey" "100"
test_command "INCR - 递增" "INCR intkey" "101"
test_command "GET - 验证递增" "GET intkey" "101"

test_command "SET浮点数值" "SET floatkey 3.14" "OK"
test_command "INCRBYFLOAT - 递增" "INCRBYFLOAT floatkey 0.5" "3.64"
test_command "GET - 验证递增" "GET floatkey" "3.64"

echo "=========================================="
echo "11. GET与APPEND组合"
echo "=========================================="
echo ""

test_command "SET初始值" "SET appendkey Hello" "OK"
test_command "APPEND - 追加内容" "APPEND appendkey \" World\"" "11"
test_command "GET - 验证追加" "GET appendkey" "Hello World"

echo "=========================================="
echo "12. 错误处理测试"
echo "=========================================="
echo ""

test_command "GET - 参数为空key" "GET \"\"" ""
test_command "GET - 空格key" "GET \" \"" ""

echo "=========================================="
echo "测试完成"
echo "=========================================="
echo ""
echo "清理测试数据..."
cleanup
echo ""
echo "所有测试完成！"
