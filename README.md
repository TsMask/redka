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
- **Unix Socket**: 支持 Unix Domain Socket 连接

## 支持的数据类型

Redka 支持 Redis 五种核心数据类型：

- **字符串 (String)** - 最基础的 Redis 类型，表示字节序列
- **列表 (List)** - 按插入顺序排序的字符串序列
- **集合 (Set)** - 无序且唯一的字符串集合
- **哈希 (Hash)** - 键值对映射
- **有序集合 (Sorted Set)** - 按关联分数排序的唯一字符串集合

Redka 还提供键管理、服务器/连接管理命令。

## 配置

Redka 支持命令行参数和 YAML 配置文件。

```bash
# 使用内存数据库启动（默认端口 6379）
./redka

# 使用配置文件启动
./redka -c config.example.yaml

# 使用文件数据库启动
./redka -p 6380 redka.db

# 使用 Unix Socket
./redka -s /tmp/redka.sock redka.db
```

### 命令行参数

```bash
./redka [-h host] [-p port] [-s unix-socket] [-a password] [-c config-file] [-v] [db-dsn]
```

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-h` | 监听地址 | `0.0.0.0` |
| `-p` | 监听端口 | `6379` |
| `-s` | Unix socket 路径（覆盖 host 和 port） | 无 |
| `-a` | 认证密码 | 无 |
| `-c` | 配置文件路径（会被命令行参数覆盖） | 无 [配置文件示例](./scripts/build/redka.yaml) |
| `-v` | 启用详细日志和性能分析端点 | `false` |
| `db-dsn` | 数据库连接字符串 | `file:/tmp/redka.sqlite?vfs=memdb` |

### DSN 格式说明

| 数据库类型 | DSN 示例 |
|------------|----------|
| SQLite | `file:redka.db` 或 `sqlite:/path/to/db` 或直接文件路径 |
| PostgreSQL | `postgres://user:password@localhost:5432/dbname` |
| MySQL | `mysql://user:password@tcp(localhost:3306)/dbname` |

## 数据库后端

Redka 使用 GORM 作为 ORM，支持三种关系型数据库后端：

- **SQLite**: 轻量级，进程内存储，适合开发和测试
- **PostgreSQL**: 功能强大的开源数据库，适合生产环境
- **MySQL**: 广泛使用的关系型数据库，支持 MariaDB

## 性能

Redka 不追求极致性能。使用 SQLite 这种通用关系型后端无法击败 Redis 这种专用数据存储。但 Redka 仍能处理每秒数万次操作，对许多应用来说足够了。

## 技术栈

- **语言**: Go 1.25+
- **协议**: RESP (Redis Serialization Protocol)
- **网络**: 基于 [Redcon](https://github.com/tidwall/redcon) 库实现
- **ORM**: [GORM](https://gorm.io/)
- **数据库驱动**: SQLite3, PostgreSQL, MySQL

## 项目结构

```text
redka/
├── redka.go               # 库入口（可作为库引用）
├── cmd/redka/main.go      # 服务入口
├── server/                # 服务器实现
├── internal/              # 内部核心模块
│   ├── core/              # 核心类型定义
│   ├── store/             # 数据库存储层
│   ├── rstring/           # 字符串操作
│   ├── rlist/             # 列表操作
│   ├── rset/              # 集合操作
│   ├── rzset/             # 有序集合操作
│   └── rhash/             # 哈希操作
├── config/                # 配置管理
├── examples/              # 库使用示例
├── scripts/               # 脚本和配置示例
└── README.md              # 本文件
```

## 库使用

Redka 可作为嵌入式库使用，以携程方式启动服务：

```go
import "github.com/tsmask/redka"

func main() {
    // 携程启动
    ready, srv := redka.StartAsync(":6379", "sqlite://test.db")
    if err := <-ready; err != nil {
        panic(err)
    }

    // 等待 shutdown 信号
    srv.WaitForShutdown()
}
```

更多示例参考 [examples/](examples/) 目录。

## 测试

```bash
# 运行功能测试脚本（需先启动服务）
sudo bash scripts/test/test_redis-cli.sh [port] [host] [password]

# 示例
sudo bash scripts/test/test_set_commands.sh 6380 localhost password
```

测试脚本会自动检测 command.go 中支持的全部命令，确保测试覆盖完整。

## 打包

```bash
# 打包 Deb 包
sudo bash scripts/build/build-deb.sh -v 2.0.0

# 打包 RPM 包
sudo bash scripts/build/build-rpm.sh -v 2.0.0

# 普通二进制
make build
```

## 贡献

欢迎贡献代码，请先开 issue 讨论你想做的更改。

欢迎使用AI工具诊断该项目，并添加或更新功能测试。

## 致谢

Redka 的实现离不开这些优秀项目及其创建者：

- [Redis](https://redis.io/) ([Salvatore Sanfilippo](https://github.com/antirez)) - 提供超越 get-set 范式的数据结构便捷 API
- [SQLite](https://sqlite.org/) ([D. Richard Hipp](https://www.sqlite.org/crew.html)) - 驱动世界的进程内数据库
- [Redcon](https://github.com/tidwall/redcon) ([Josh Baker](https://github.com/tidwall)) - 简洁易用的 RESP 服务器实现
- [GORM](https://gorm.io/) - Go 语言的 ORM 库
- [Fork nalgeon/redka](https://github.com/nalgeon/redka) - Redka用SQL重新实现，与Redis API保持兼容

Logo 字体由 [Ek Type](https://ektype.in/) 提供。
