# PebbleDB PostgreSQL-Compatible Server

This implementation provides a PostgreSQL-compatible server interface for the PebbleDB-based database, along with an embedded client library.

## Features

1. **PostgreSQL Wire Protocol Server** - Compatible with standard PostgreSQL clients
2. **HTTP REST API** - Alternative interface for web applications
3. **Embedded Client Library** - Direct access for applications that embed the database
4. **Multi-tenancy Support** - Isolated data storage for different tenants
5. **SQL Parser and Planner** - Basic SQL support using xwb1989/sqlparser

## Usage

### Starting the Server

```bash
# Start HTTP REST API server with custom database path
go run cmd/server/main.go /path/to/db

# Start PostgreSQL wire protocol server with custom database path
go run cmd/server/main.go /path/to/db pg
```

### Using the Embedded Client

```go
import "github.com/guileen/pglitedb/client"

// Create an embedded client
db := client.NewClient("/path/to/db")

// Insert a record
data := map[string]interface{}{
    "name":  "John Doe",
    "email": "john@example.com",
    "age":   30,
}
result, err := db.Insert(ctx, tenantID, "users", data)

// Query records
options := &table.QueryOptions{
    Where: map[string]interface{}{
        "age": 30,
    },
    Limit: intPtr(10),
}
result, err := db.Select(ctx, tenantID, "users", options)
```

## Architecture

The implementation follows a layered architecture:

1. **Server Layer** - HTTP REST API and PostgreSQL wire protocol servers
2. **Client Layer** - Embedded client library with unified interface
3. **Executor Layer** - Query execution and planning
4. **Storage Layer** - PebbleDB-based storage engine with multi-tenancy
5. **SQL Layer** - SQL parsing and planning capabilities

## Dependencies

- github.com/jackc/pgx/v5 - PostgreSQL wire protocol implementation
- github.com/xwb1989/sqlparser - SQL parsing
- github.com/cockroachdb/pebble - Embedded key-value store
- github.com/go-chi/chi/v5 - HTTP router

## Limitations

This is a simplified implementation for demonstration purposes. A production implementation would need:

1. Full SQL support including DDL operations
2. Comprehensive PostgreSQL compatibility
3. Authentication and authorization
4. Connection pooling
5. Performance optimizations
6. Extensive testing