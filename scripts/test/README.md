# Redis 命令测试脚本

## 📋 概述

本目录包含redka的Redis命令测试脚本，用于全面测试各种Redis命令实现。

## 📦 测试脚本列表

### 1. SET命令测试
```bash
./test_set_commands.sh [port] [host] [password]
```

**测试内容**：
- ✅ 基本SET/GET操作
- ✅ NX选项（只在key不存在时设置）
- ✅ XX选项（只在key存在时更新）
- ✅ GET选项（返回旧值）
- ✅ EX/PX/EXAT/PXAT选项（过期时间）
- ✅ KEEPTTL选项（保持TTL）
- ✅ NX+GET/XX+GET组合
- ✅ NX+EX/XX+EX组合
- ✅ 边界条件测试
- ✅ 类型检查测试
- ✅ 原子性测试

### 2. GET命令测试
```bash
./test_get_commands.sh [port] [host] [password]
```

**测试内容**：
- ✅ 基本GET操作
- ✅ GET不存在的key
- ✅ GET过期key（惰性删除）
- ✅ GET非字符串类型key
- ✅ MGET批量操作
- ✅ GET特殊值（大数值、特殊字符、空格）
- ✅ GETRANGE子字符串操作
- ✅ TTL和GET组合
- ✅ 原子性保证
- ✅ GET与类型转换（INCR/INCRBYFLOAT）
- ✅ GET与APPEND组合
- ✅ 错误处理

## 🚀 使用方法

### 基本使用

```bash
# 测试本地Redis（默认6379端口）
cd /home/manager/redka/scripts/test
./test_set_commands.sh
./test_get_commands.sh
```

### 带密码认证

```bash
./test_set_commands.sh 6379 localhost helloearth
./test_get_commands.sh 6379 localhost helloearth
```

### 测试远程Redis

```bash
./test_set_commands.sh 6379 192.168.1.100 password123
./test_get_commands.sh 6379 192.168.1.100 password123
```

## 📊 参数说明

| 参数 | 位置 | 默认值 | 说明 |
|------|------|--------|------|
| PORT | 第1个 | 6379 | Redis服务端口 |
| HOST | 第2个 | localhost | Redis服务地址 |
| PASSWORD | 第3个 | (无) | Redis密码认证 |

## 🎯 测试覆盖范围

### SET命令测试（35项测试）

| 测试类别 | 测试项数 | 主要内容 |
|---------|---------|---------|
| 基本操作 | 4项 | SET、GET、覆盖 |
| NX选项 | 3项 | 不存在时设置、已存在时拒绝 |
| XX选项 | 3项 | 存在时更新、不存在时拒绝 |
| GET选项 | 3项 | 返回旧值、创建新key |
| 过期选项 | 8项 | EX、PX、EXAT、PXAT |
| 组合选项 | 6项 | NX+GET、XX+GET、NX+EX、XX+EX |
| 边界测试 | 5项 | 空值、特殊字符、大数值 |
| 类型检查 | 3项 | Hash、List、Set的WRONGTYPE错误 |
| 原子性 | 4项 | GET原子性保证 |

### GET命令测试（30项测试）

| 测试类别 | 测试项数 | 主要内容 |
|---------|---------|---------|
| 基本操作 | 4项 | SET、GET、修改 |
| 不存在key | 3项 | nil返回、空值处理 |
| 过期key | 2项 | 惰性删除验证 |
| 非字符串类型 | 3项 | Hash、List、Set类型检查 |
| 批量操作 | 4项 | MGET多key、包含不存在key |
| 特殊值 | 5项 | 大数值、特殊字符、空格、数字 |
| 子字符串 | 5项 | GETRANGE各种范围 |
| TTL组合 | 2项 | TTL和GET配合 |
| 类型转换 | 3项 | INCR、INCRBYFLOAT |
| APPEND组合 | 2项 | 追加后GET |
| 错误处理 | 2项 | 空key、参数验证 |

## 📝 输出示例

### 成功示例
```
----------------------------------------
测试: 基本GET - 获取字符串值
命令: GET get1
结果: hello
期望: hello
✅ 通过
```

### 失败示例
```
----------------------------------------
测试: GET - 已过期的key返回nil
命令: GET get2
结果: expirevalue
期望: (空)
❌ 失败
```

## 🔍 调试建议

### 查看详细输出

```bash
# 运行测试并保存完整输出
./test_set_commands.sh 2>&1 | tee test_output.log
./test_get_commands.sh 2>&1 | tee test_output.log
```

### 只运行特定测试

```bash
# 手动测试单个命令
redis-cli -h localhost -p 6379
> GET mykey
> SET mykey "test value"
> GET mykey
```

### 检查连接

```bash
# 测试连接
redis-cli -h localhost -p 6379 -a yourpassword --no-auth-warning ping
# 应该返回: PONG
```

## 📈 性能基准

每个测试脚本执行时间：
- `test_set_commands.sh`: ~10秒（含TTL等待）
- `test_get_commands.sh`: ~5秒（含TTL等待）

## 🤝 贡献

如果需要添加新的测试用例：

1. 在对应的测试函数中添加测试项
2. 使用现有的`test_command`函数
3. 确保清理测试数据

示例：
```bash
test_command "新测试项" "COMMAND args" "expected_result"
```

## 📚 相关文档

- [SET命令官方文档](https://redis.io/docs/latest/commands/set/)
- [GET命令官方文档](https://redis.io/docs/latest/commands/get/)
- [MGET命令官方文档](https://redis.io/docs/latest/commands/mget/)
- [GETRANGE命令官方文档](https://redis.io/docs/latest/commands/getrange/)

## ⚠️ 注意事项

1. **数据清理**：测试脚本会自动清理测试数据，但建议在测试前手动清理
2. **TTL等待**：某些测试需要等待TTL过期，脚本会自动sleep
3. **并发测试**：不建议在生产环境运行测试脚本
4. **网络延迟**：MGET等批量操作可能受网络影响

## 🎉 快速开始

```bash
# 1. 编译redka
cd /home/manager/redka
go build -o redka .

# 2. 启动redka服务
./redka -conf scripts/redka.yaml

# 3. 新开终端，运行测试
cd scripts/test
./test_set_commands.sh 6379 localhost yourpassword
./test_get_commands.sh 6379 localhost yourpassword
```

祝测试愉快！🚀
