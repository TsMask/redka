<img alt="Redka" src="logo.svg" height="80" align="center">

> This project is based on the secondary modification of [fork nalgeon/redka](https://github.com/nalgeon/redka)

Redka 是一个用 Go 语言实现的 Redis 兼容服务器，使用关系型数据库作为后端存储，同时保持与 Redis API 的完全兼容。

## 特性

- **内存高效**: 数据无需全部加载到内存，支持持久化存储
- **ACID 事务**: 提供完整的事务支持，确保数据一致性
- **SQL 视图**: 提供 SQL 视图便于数据分析和报表生成
- **多数据库支持**: 支持 SQLite、PostgreSQL 或 MySQL 作为后端存储
- **灵活部署**: 可嵌入 Go 应用使用，或作为独立服务器运行
- **Redis 兼容**: 完整实现 Redis 命令和 RESP 通信协议
- **认证支持**: 支持密码认证，保障服务安全
- **配置管理**: 支持 YAML 配置文件和命令行参数
- **日志系统**: 提供结构化日志，支持文件输出和详细日志模式

## 使用场景

**嵌入式缓存**: 如果你的 Go 应用已使用 SQLite 或需要一个内置键值存储，Redka 是理想选择。除基本的 get/set 和过期设置外，还支持列表、哈希、集合等高级数据结构。

**轻量级测试环境**: 生产环境使用 Redis，但为本地开发或集成测试设置 Redis 服务器较为繁琐。Redka 内存数据库可作为测试容器的快速替代方案，为每次测试提供完全隔离。

**PostgreSQL/MySQL 优先**: 如果你偏好用 PostgreSQL 或 MySQL 处理一切但需要 Redis 数据结构，Redka 可使用现有数据库作为后端，用相同工具和事务保证管理关系型数据和专用数据结构。

## 支持的数据类型

Redka 支持五种 Redis 核心数据类型：

- [字符串](docs/commands/strings.md) - 最基础的 Redis 类型，表示字节序列
- [列表](docs/commands/lists.md) - 按插入顺序排序的字符串序列
- [集合](docs/commands/sets.md) - 无序且唯一的字符串集合
- [哈希](docs/commands/hashes.md) - 键值对映射
- [有序集合](docs/commands/sorted-sets.md) - 按关联分数排序的唯一字符串集合

Redka 还提供[键管理](docs/commands/keys.md)、[服务器/连接管理](docs/commands/server.md)和[事务](docs/commands/transactions.md)命令。

## 快速开始

### 独立服务器模式

Redka 可以作为独立的 Redis 兼容服务器运行：

```bash
# 使用内存数据库启动
./redka

# 使用文件数据库启动
./redka redka.db

# 使用配置文件启动
./redka -c config.yaml

# 监听所有网络接口
./redka -h 0.0.0.0 -p 6379 redka.db
```

### Docker 部署

```bash
# 使用默认配置运行
docker run --rm -p 6379:6379 tsmask/redka

# 挂载数据目录实现持久化
docker run --rm -p 6379:6379 -v /path/to/data:/data tsmask/redka
```

### Go 模块嵌入

在 Go 项目中作为库使用：

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

Redka 支持多种配置方式：

- 命令行参数
- 环境变量
- YAML 配置文件

示例配置文件：

```yaml
host: localhost
port: 6379
db_dsn: "file:redka.db?vfs=memdb"
password: ""
verbose: false
log_file: ""
```

详细安装和使用说明：
- [独立服务器安装](docs/install-standalone.md)
- [独立服务器使用](docs/usage-standalone.md)
- [Go 模块安装](docs/install-module.md)
- [Go 模块使用](docs/usage-module.md)

## 数据库后端

Redka 可使用 SQLite、PostgreSQL 或 MySQL 作为后端。数据存储在[关系型数据库](docs/persistence.md)中，使用简单模式并提供视图以便更好地检查数据。

## 性能

Redka 不追求极致性能。无法用 SQLite 这种通用关系型后端击败 Redis 这种专用数据存储。但 Redka 仍能处理每秒数万次操作，对许多应用来说足够了。

更多详情请查看[性能测试](docs/performance.md)。

## 技术栈

- **语言**: Go 1.25+
- **协议**: RESP (Redis Serialization Protocol)
- **网络**: 基于 Redcon 库实现
- **ORM**: GORM
- **数据库驱动**: SQLite3, PostgreSQL, MySQL

## 贡献

欢迎贡献代码。除 bug 修复外，请先开 issue 讨论你想做的更改。

请根据需要添加或更新测试。

## 致谢

Redka 的实现离不开这些优秀项目及其创建者：

- [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez))。提供超越 get-set 范式的数据结构便捷 API，真是绝妙的想法。
- [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html))。驱动世界的进程内数据库。
- [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall))。非常简洁易用的 RESP 服务器实现。
- [GORM](https://gorm.io/) - Go 语言的 ORM 库

Logo 字体由 [Ek Type](https://ektype.in/) 提供。
