# PGLiteDB 文档 - 中文

欢迎使用 PGLiteDB 官方文档，这是一个高性能的 PostgreSQL 兼容嵌入式数据库。

## 关于 PGLiteDB

PGLiteDB 是一款尖端的嵌入式数据库，提供完全的 PostgreSQL 线协议兼容性，同时提供卓越的性能。基于 CockroachDB 的 Pebble 存储引擎（基于 LSM 树的键值存储），PGLiteDB 提供了开发人员喜爱的熟悉的 PostgreSQL 接口，并具有现代应用程序所需的性能特征。

在基准测试中，PGLiteDB 的 TPS 超过 3,100，延迟低于 3.2 毫秒，性能优于传统嵌入式数据库，同时保持 PostgreSQL 兼容性。最新的优化通过对象池、批处理操作和零分配编码技术将关键操作的内存分配减少了高达 90%。带有 LRU 驱逐的查询计划缓存为重复查询提供了 3 倍的性能提升。这些改进有助于在高负载条件下保持一致的性能。

## 快速链接

- [快速入门指南](./guides/quickstart.md) - 不同用例的逐步指南
- [安装指南](../../README.md#installation) - 如何安装和设置 PGLiteDB
- [API 参考](./api/reference.md) - 所有公共 API 的详细文档
- [示例](./guides/interactive_examples.md) - 展示关键功能的可运行示例
- [性能优化](./performance_optimizations.md) - 最新性能优化详情
- [架构文档](../architecture.md) - 详细架构信息
- [组件结构](../component_structure.md) - 组件交互详情
- [资源管理](../resource_management.md) - 资源管理策略

## 语言版本

- [English](../README.md) - English documentation
- [中文](./README.md) - 中文文档
- [Español](../es/README.md) - Documentación en español
- [日本語](../ja/README.md) - 日本語ドキュメント

## 核心特性

### 🚀 高性能
- **⚡ 高吞吐量** - 约 3,100 TPS
- **⏱️ 低延迟** - 约 3.2ms 延迟
- **💾 内存效率** - 通过对象池将内存分配减少高达 90%
- **🔄 连接池** - 带健康检查的高效连接管理
- **キャッシング** - 带 LRU 驱逐的查询计划缓存，提供 3 倍性能提升

### 🔌 PostgreSQL 兼容性
- **📋 完整 SQL 支持** - 标准 SQL 操作（SELECT, INSERT, UPDATE, DELETE）
- **🗃️ 高级索引** - 二级索引支持 B 树和哈希实现
- **🏢 多租户** - 内置租户隔离，适用于 SaaS 应用
- **🛡️ ACID 合规** - 完整事务支持，含 MVCC 和所有隔离级别
- **🔄 事务管理** - 高级死锁检测和预防，保存点支持

### 📦 嵌入式和服务器模式
- **🧩 嵌入式库** - 单二进制部署，无外部依赖
- **🖥️ 独立服务器** - 可作为独立服务器运行
- **🌐 多协议访问** - PostgreSQL 线协议、HTTP REST API 和原生 Go 客户端

### 🧠 智能优化
- **🔁 对象池** - 减少垃圾回收开销
- **📦 批处理操作** - 提高批量操作效率
- **🔗 连接池** - 高效连接管理
- **キャッシング** - 查询计划缓存提高重复查询性能
- **📊 成本优化** - 基于成本的查询优化

## 架构概览

PGLiteDB 采用分层架构设计，各层之间职责清晰：

```
┌─────────────────────────────────────────────────────────────┐
│                    应用层                                     │
├─────────────────────┬─────────────────┬─────────────────────┤
│  PostgreSQL客户端   │  HTTP REST API  │  嵌入式客户端          │
│  (psql, pg, pgx)    │  (curl, fetch)  │  (Go SDK)           │
└─────────────────────┴─────────────────┴─────────────────────┘
           │                   │                   │
           └───────────────────┼───────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      协议层                                   │
│  ┌──────────────────┐      ┌──────────────────┐             │
│  │  PG线协议         │      │   REST处理器      │             │
│  │   (pgserver)     │      │   (api/rest)     │             │
│  └──────────────────┘      └──────────────────┘             │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      执行层                                   │
│  ┌──────────────────────────────────────────────┐            │
│  │  SQL解析器 → 规划器 → 执行器                  │            │
│  │  (protocol/sql)                              │            │
│  └──────────────────────────────────────────────┘            │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      引擎层                                   │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  表管理器        │  │  索引管理器       │                   │
│  │  (engine/table) │  │  (engine/engine)│                   │
│  └─────────────────┘  └─────────────────┘                   │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      存储层                                   │
│  ┌──────────────────────────────────────────────┐            │
│  │  Pebble键值存储 (storage)                   │            │
│  │  - 多租户支持                                │            │
│  │  - 内存可比较编码 (codec)                     │            │
│  └──────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

## 性能基准

| 数据库 | TPS | 延迟 | 内存使用 |
|--------|-----|------|----------|
| PGLiteDB | 2,482 | ~4.03ms | 优化 |
| PostgreSQL | 2272 | 4.40ms | 200MB+ |
| SQLite | 1800 | 5.55ms | 120MB |

PGLiteDB 在提供完整 PostgreSQL 兼容性的同时，为嵌入式用例提供优化性能。

## 快速开始

### 安装

```bash
go get github.com/guileen/pglitedb
```

### 启动服务器

```bash
# 启动 PostgreSQL 线协议服务器（默认端口 5432）
go run cmd/server/main.go /path/to/db pg

# 启动 HTTP REST API 服务器（默认端口 8080）
go run cmd/server/main.go /path/to/db
```

### 使用嵌入式客户端

```go
package main

import (
    "context"
    "fmt"
    "log"
    
    "github.com/guileen/pglitedb/client"
    "github.com/guileen/pglitedb/types"
)

func main() {
    // 创建嵌入式客户端
    db := client.NewClient("/path/to/db")
    ctx := context.Background()
    tenantID := uint64(1)
    
    // 插入记录
    data := map[string]interface{}{
        "name":  "张三",
        "email": "zhangsan@example.com",
        "age":   30,
    }
    result, err := db.Insert(ctx, tenantID, "users", data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("插入了 %d 行\n", result.Count)
    
    // 查询记录
    options := &types.QueryOptions{
        Where: map[string]interface{}{
            "age": 30,
        },
        OrderBy: []types.OrderByClause{
            {Column: "name", Desc: false},
        },
        Limit: intPtr(10),
    }
    result, err = db.Select(ctx, tenantID, "users", options)
    if err != nil {
        log.Fatal(err)
    }
    
    for _, row := range result.Rows {
        fmt.Printf("用户: %+v\n", row)
    }
}

func intPtr(i int) *int {
    return &i
}
```

## 测试

### 运行所有测试

```bash
# 运行单元测试
go test ./...

# 运行带覆盖率的测试
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

## 贡献

欢迎贡献！请随时提交问题或拉取请求。

### 贡献领域

我们正在积极寻求贡献者帮助使 PGLiteDB 成为最佳的 PostgreSQL 兼容嵌入式数据库。以下是需要您帮助的领域：

1. **性能优化** - 帮助我们从引擎中榨取更多性能
2. **SQL 合规性** - 扩展我们的 PostgreSQL 兼容性
3. **文档** - 改进示例和教程
4. **测试** - 添加更多测试用例和边缘条件
5. **功能** - 实现与我们路线图一致的新功能

## 许可证

弹性许可证 2.0 - 有关详细信息，请参见 LICENSE 文件