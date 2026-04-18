# Redka Examples

本目录包含 Redka 作为库使用的示例。

## basic

基础示例，展示最简单的携程启动方式。

```bash
go run ./examples/basic
```

使用 SQLite 内存数据库，监听端口 6380。

## custom

自定义配置示例，展示如何设置密码、限制客户端数等。

```bash
go run ./examples/custom
```

监听端口 6380，使用 SQLite 文件存储。

## 库使用方式

```go
import "github.com/tsmask/redka"

// 携程启动（推荐）
ready, srv := redka.StartAsync(":6380", "sqlite://test.db")
<-ready
srv.WaitForShutdown()

// 或同步启动
srv, _ := redka.Start(":6380", "postgres://user:pass@host/db")
srv.WaitForShutdown()
```

## DSN 格式

- SQLite: `sqlite://test.db` 或 `:memory:` 或 `sqlite:/tmp/redka.sqlite?vfs=memdb`
- PostgreSQL: `postgres://user:pass@host/db`
- MySQL: `mysql://user:pass@tcp(host)/db`
