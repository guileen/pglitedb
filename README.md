# PGLiteDB - High-Performance PostgreSQL-Compatible Embedded Database

PGLiteDB is a lightweight, embedded database with PostgreSQL-compatible interfaces. It combines the simplicity of embedded databases with the power of SQL and PostgreSQL wire protocol compatibility, delivering high performance through advanced optimization techniques.

## Features

- **PostgreSQL Wire Protocol Server** - Connect using any PostgreSQL client (psql, pg, node-postgres, etc.)
- **HTTP REST API** - RESTful interface for web applications
- **Embedded Client Library** - Direct Go client for embedded use cases
- **Multi-tenancy Support** - Isolated data storage for different tenants
- **SQL Support** - Standard SQL operations (SELECT, INSERT, UPDATE, DELETE)
- **Indexing** - Secondary indexes for query optimization
- **High Performance** - Optimized storage engine with object pooling and batch operations
- **Storage Engine** - Built on CockroachDB's Pebble (LSM-tree based key-value store)

## Quick Start

### Installation

```bash
go get github.com/guileen/pglitedb
```

### Starting the Server

```bash
# Start PostgreSQL wire protocol server (default port 5432)
go run cmd/server/main.go /path/to/db pg

# Start HTTP REST API server (default port 8080)
go run cmd/server/main.go /path/to/db
```

### Using the Embedded Client

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
    // Create an embedded client
    db := client.NewClient("/path/to/db")
    ctx := context.Background()
    tenantID := uint64(1)
    
    // Insert a record
    data := map[string]interface{}{
        "name":  "John Doe",
        "email": "john@example.com",
        "age":   30,
    }
    result, err := db.Insert(ctx, tenantID, "users", data)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Inserted %d rows\n", result.Count)
    
    // Query records
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
        fmt.Printf("User: %+v\n", row)
    }
}

func intPtr(i int) *int {
    return &i
}
```

### Using with PostgreSQL Clients

```bash
# Using psql
psql -h localhost -p 5432 -U postgres

# Or with node-postgres
npm install pg
```

```javascript
const { Client } = require('pg');

const client = new Client({
  host: 'localhost',
  port: 5432,
  database: 'postgres',
  user: 'postgres',
});

await client.connect();
const res = await client.query('SELECT * FROM users WHERE age > $1', [25]);
console.log(res.rows);
await client.end();
```

### Using the HTTP REST API

```bash
# Insert a record
curl -X POST http://localhost:8080/api/v1/tenants/1/tables/users/insert \
  -H "Content-Type: application/json" \
  -d '{"name": "Jane Doe", "email": "jane@example.com", "age": 28}'

# Query records
curl -X POST http://localhost:8080/api/v1/tenants/1/tables/users/select \
  -H "Content-Type: application/json" \
  -d '{"where": {"age": 28}, "limit": 10}'
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    Application Layer                        │
├─────────────────────┬─────────────────┬─────────────────────┤
│  PostgreSQL Client  │  HTTP REST API  │  Embedded Client    │
│  (psql, pg, pgx)    │  (curl, fetch)  │  (Go SDK)           │
└─────────────────────┴─────────────────┴─────────────────────┘
           │                   │                   │
           └───────────────────┼───────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      Protocol Layer                          │
│  ┌──────────────────┐      ┌──────────────────┐             │
│  │  PG Wire Protocol│      │   REST Handler   │             │
│  │   (pgserver)     │      │   (api/rest)     │             │
│  └──────────────────┘      └──────────────────┘             │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      Executor Layer                          │
│  ┌──────────────────────────────────────────────┐            │
│  │  SQL Parser → Planner → Executor             │            │
│  │  (protocol/sql)                              │            │
│  └──────────────────────────────────────────────┘            │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      Engine Layer                            │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  Table Manager  │  │  Index Manager  │                   │
│  │  (engine/table) │  │  (engine/engine)│                   │
│  └─────────────────┘  └─────────────────┘                   │
└──────────────────────────────┬──────────────────────────────┘
                               │
┌──────────────────────────────▼──────────────────────────────┐
│                      Storage Layer                           │
│  ┌──────────────────────────────────────────────┐            │
│  │  Pebble KV Store (storage)                   │            │
│  │  - Multi-tenancy support                     │            │
│  │  - Memory-comparable encoding (codec)        │            │
│  └──────────────────────────────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

### Layer Responsibilities

- **Protocol Layer**: Handle client connections (PostgreSQL wire protocol, HTTP REST API)
- **Executor Layer**: Parse SQL, plan queries, execute operations
- **Engine Layer**: Table and index management, query optimization
- **Storage Layer**: Key-value storage, encoding/decoding, multi-tenancy isolation

## Testing

### Run All Tests

```bash
# Run unit tests
go test ./...

# Run tests with coverage
go test ./... -coverprofile=coverage.out
go tool cover -html=coverage.out
```

### Integration Tests

```bash
# Test embedded client
cd examples/embedded_client
go run main.go

# Test PostgreSQL compatibility
cd examples/compatibility_test
go test -v

# Test TypeScript client (requires server running)
cd examples/typescript_test
npm install
npm test
```

### Performance Tests

See [examples/benchmark](examples/benchmark) for performance testing tools and results.

Recent benchmark results show over 2200 transactions per second with sub-5ms latency, demonstrating the high performance of the optimized storage engine.

## Project Structure

```
pglitedb/
├── client/           # Embedded client library
├── protocol/         # Protocol implementations
│   ├── api/          # HTTP REST API handlers
│   ├── pgserver/     # PostgreSQL wire protocol server
│   ├── sql/          # SQL parser and executor
│   └── executor/     # Query executor
├── engine/           # Database engine
│   ├── engine/       # Core engine and indexing
│   ├── manager/      # Engine manager
│   └── table/        # Table management
├── storage/          # Storage layer (Pebble KV)
├── codec/            # Data encoding/decoding
├── types/            # Common types
├── cmd/              # Command-line tools
│   └── server/       # Server executable
└── examples/         # Example code and tests
    ├── embedded_client/
    ├── compatibility_test/
    ├── typescript_test/
    └── benchmark/
```

## Dependencies

- [github.com/cockroachdb/pebble](https://github.com/cockroachdb/pebble) - High-performance LSM-tree key-value store
- [github.com/jackc/pgx/v5](https://github.com/jackc/pgx) - PostgreSQL wire protocol
- [github.com/pganalyze/pg_query_go/v6](https://github.com/pganalyze/pg_query_go) - PostgreSQL query parsing
- [github.com/go-chi/chi/v5](https://github.com/go-chi/chi) - HTTP router

## Limitations & Future Work

This is an experimental project for learning and demonstration. Production use would require:

1. **SQL Features**
   - DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE)
   - Joins and subqueries
   - Transactions (BEGIN, COMMIT, ROLLBACK)
   - Constraints (PRIMARY KEY, FOREIGN KEY, UNIQUE)
   
2. **Performance**
   - Query optimization
   - Connection pooling
   - Parallel query execution
   - Better index strategies
   - Advanced caching mechanisms

3. **Reliability**
   - Write-ahead logging (WAL)
   - Crash recovery
   - Data replication
   - Backup and restore

4. **Security**
   - Authentication and authorization
   - SSL/TLS support
   - Row-level security
   - SQL injection prevention

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

## License

MIT License - see LICENSE file for details