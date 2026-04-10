# Redka

<img alt="Redka" src="logo.svg" height="80" align="center">

> 本项目基于 [fork nalgeon/redka](https://github.com/nalgeon/redka) 二次开发

Redka 是一个用 Go 语言实现的 Redis 兼容服务器，使用关系型数据库作为后端存储，同时保持与 Redis API 的完全兼容。

## 特性

- **多后端支持**: 支持 SQLite、PostgreSQL、MySQL 作为后端存储
- **内存高效**: 数据无需全部加载到内存，支持持久化存储
- **ACID 事务**: 提供完整的事务支持，确保数据一致性
- **SQL 视图**: 提供 SQL 视图便于数据分析和报表生成
- **灵活部署**: 可嵌入 Go 应用使用，或作为独立服务器运行
- **Redis 兼容**: 完整实现 Redis 命令和 RESP 通信协议
- **认证支持**: 支持密码认证，保障服务安全
- **配置管理**: 支持 YAML 配置文件和命令行参数
- **日志系统**: 提供结构化日志，支持文件输出和详细日志模式

## 支持的数据类型

Redka 支持 Redis 五种核心数据类型：

- **字符串 (String)** - 最基础的 Redis 类型，表示字节序列
- **列表 (List)** - 按插入顺序排序的字符串序列
- **集合 (Set)** - 无序且唯一的字符串集合
- **哈希 (Hash)** - 键值对映射
- **有序集合 (Sorted Set)** - 按关联分数排序的唯一字符串集合

Redka 还提供键管理、服务器/连接管理命令。

## 快速开始

### 独立服务器模式

```bash
# 使用内存数据库启动（默认端口 6379）
./redka

# 使用文件数据库启动
./redka redka.db

# 使用配置文件启动
./redka -c config.example.yaml

# 监听所有网络接口
./redka -h 0.0.0.0 -p 6380 redka.db
```

### Go 模块嵌入

```go
import "github.com/tsmask/redka"

// 打开数据库
db, err := redka.Open("redka.db", nil)
if err != nil {
    log.Fatal(err)
}
defer db.Close()

// 使用字符串操作
db.Str().Set("name", []byte("alice"))
val, _ := db.Str().Get("name")
```

## 配置

Redka 支持命令行参数和 YAML 配置文件。

### 命令行参数

```bash
./redka -h 0.0.0.0 -p 6380 -c config.yaml 

```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-h` | 监听地址 | `localhost` |
| `-p` | 监听端口 | `6379` |
| `-v` | 启用详细日志 | `false` |
| `-c` | 配置文件路径（会覆盖命令行参数） | 无 |
| 后端地址 | 数据源链接 | `file:/redka.db?vfs=memdb` |

### 配置文件示例

```yaml
host: localhost
port: 6380
db_dsn: "file:/redka.db?vfs=memdb"
password: ""
verbose: false
log_file: ""
```

### DSN 格式说明

| 数据库类型 | DSN 示例 |
|------------|----------|
| SQLite | `file:redka.db` 或 `sqlite:/path/to/db` |
| PostgreSQL | `postgres://user:password@localhost:5432/dbname` |
| MySQL | `mysql://user:password@tcp(localhost:3306)/dbname` |

## 支持的命令

Redka 实现了以下 Redis 命令分类：

### 连接与服务器
`PING`, `ECHO`, `SELECT`, `AUTH`, `COMMAND`, `CONFIG`, `DBSIZE`, `FLUSHDB`, `FLUSHALL`, `INFO`, `LOLWUT`

### 键操作
`DEL`, `EXISTS`, `EXPIRE`, `EXPIREAT`, `PEXPIRE`, `PEXPIREAT`, `KEYS`, `PERSIST`, `RANDOMKEY`, `RENAME`, `RENAMENX`, `SCAN`, `TTL`, `TYPE`

### 字符串
`GET`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETSET`, `MGET`, `MSET`, `INCR`, `INCRBY`, `DECR`, `DECRBY`, `INCRBYFLOAT`, `APPEND`, `STRLEN`, `GETRANGE`, `SETRANGE`, `SUBSTR`

### 列表
`LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LINSERT`, `LREM`, `LTRIM`, `RPOPLPUSH`

### 集合
`SADD`, `SREM`, `SCARD`, `SISMEMBER`, `SMEMBERS`, `SMISMEMBER`, `SPOP`, `SRANDMEMBER`, `SUNION`, `SINTER`, `SDIFF`, `SINTERSTORE`, `SUNIONSTORE`, `SDIFFSTORE`, `SINTERCARD`, `SMOVE`, `SSCAN`

### 有序集合
`ZADD`, `ZREM`, `ZCARD`, `ZSCORE`, `ZRANK`, `ZREVRANK`, `ZRANGE`, `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZCOUNT`, `ZINCRBY`, `ZUNION`, `ZINTER`, `ZUNIONSTORE`, `ZINTERSTORE`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZSCAN`

### 哈希
`HSET`, `HGET`, `HGETALL`, `HMGET`, `HMSET`, `HDEL`, `HLEN`, `HEXISTS`, `HSETNX`, `HINCRBY`, `HINCRBYFLOAT`, `HKEYS`, `HVALS`, `HSTRLEN`, `HRANDFIELD`, `HSCAN`

## 数据库后端

Redka 使用 GORM 作为 ORM，支持三种关系型数据库后端：

- **SQLite**: 轻量级，进程内存储，适合开发和测试
- **PostgreSQL**: 功能强大的开源数据库，适合生产环境
- **MySQL**: 广泛使用的关系型数据库，支持 MariaDB

数据存储在关系型数据库中，使用简单模式并提供视图以便检查数据。

## 性能

Redka 不追求极致性能。使用 SQLite 这种通用关系型后端无法击败 Redis 这种专用数据存储。但 Redka 仍能处理每秒数万次操作，对许多应用来说足够了。

## 技术栈

- **语言**: Go 1.25+
- **协议**: RESP (Redis Serialization Protocol)
- **网络**: 基于 [Redcon](https://github.com/tidwall/redcon) 库实现
- **ORM**: [GORM](https://gorm.io/)
- **数据库驱动**: SQLite3, PostgreSQL, MySQL

## 项目结构

```
redka/
├── cmd/redka/main.go      # 服务入口
├── redsrv/                # 服务器实现
├── internal/              # 内部核心模块
├── config/                # 配置加载
├── docs/                  # 文档
├── test-redis-cli.sh      # 功能测试脚本
└── README.md             # 本文件
```

## 测试

```bash
# 运行功能测试脚本（需先启动服务）
./test-redis-cli.sh [port] [host]

# 示例
./test-redis-cli.sh 6380 127.0.0.1
```

测试脚本会自动检测 command.go 中支持的全部命令，确保测试覆盖完整。

## 贡献

欢迎贡献代码。除 bug 修复外，请先开 issue 讨论你想做的更改。

请根据需要添加或更新测试。

## 致谢

Redka 的实现离不开这些优秀项目及其创建者：

- [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez)) - 提供超越 get-set 范式的数据结构便捷 API
- [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html)) - 驱动世界的进程内数据库
- [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall)) - 简洁易用的 RESP 服务器实现
- [GORM](https://gorm.io/) - Go 语言的 ORM 库

Logo 字体由 [Ek Type](https://ektype.in/) 提供。