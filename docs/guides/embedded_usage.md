# PGLiteDB Embedded Usage Guide

This guide explains how to use PGLiteDB as an embedded database in your Go applications. PGLiteDB provides full PostgreSQL compatibility while running as an embedded library, offering exceptional performance and ease of use.

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Basic Usage](#basic-usage)
4. [Advanced Operations](#advanced-operations)
5. [Transaction Management](#transaction-management)
6. [Multi-tenancy](#multi-tenancy)
7. [Performance Considerations](#performance-considerations)
8. [Error Handling](#error-handling)

## Introduction

PGLiteDB is a high-performance embedded database that offers full PostgreSQL wire protocol compatibility. Unlike traditional embedded databases, PGLiteDB provides the familiar PostgreSQL interface that developers love while delivering exceptional performance.

Key benefits of using PGLiteDB in embedded mode:
- **Performance**: Over 2500 TPS with sub-4ms latency
- **Compatibility**: Full PostgreSQL SQL support
- **Simplicity**: Single binary deployment with no external dependencies
- **Multi-tenancy**: Built-in tenant isolation for SaaS applications
- **ACID Compliance**: Full transaction support with MVCC

## Installation

To use PGLiteDB in your Go application, add it as a dependency:

```bash
go get github.com/guileen/pglitedb
```

## Basic Usage

### Creating a Client

To start using PGLiteDB, create an embedded client instance:

```go
package main

import (
    "context"
    "log"
    
    "github.com/guileen/pglitedb/client"
)

func main() {
    // Create an embedded client
    db := client.NewClient("/path/to/database")
    ctx := context.Background()
    
    // Your database operations here...
}
```

### Executing Raw SQL Queries

You can execute raw SQL queries using the `Query` method:

```go
result, err := db.Query(ctx, "CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(100), email VARCHAR(100))")
if err != nil {
    log.Fatal(err)
}

result, err = db.Query(ctx, "INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')")
if err != nil {
    log.Fatal(err)
}

result, err = db.Query(ctx, "SELECT * FROM users WHERE name = 'John Doe'")
if err != nil {
    log.Fatal(err)
}

for _, row := range result.Rows {
    log.Printf("User: %+v", row)
}
```

### Using Structured Operations

PGLiteDB provides structured methods for common CRUD operations:

#### Insert Records

```go
data := map[string]interface{}{
    "name":  "Jane Smith",
    "email": "jane@example.com",
    "age":   28,
}

result, err := db.Insert(ctx, 1, "users", data)
if err != nil {
    log.Fatal(err)
}
log.Printf("Inserted record with ID: %d", result.LastInsertID)
```

#### Query Records

```go
options := &types.QueryOptions{
    Where: map[string]interface{}{
        "age": 28,
    },
    OrderBy: []string{"name ASC"},
    Limit:   intPtr(10),
}

result, err := db.Select(ctx, 1, "users", options)
if err != nil {
    log.Fatal(err)
}

for _, row := range result.Rows {
    log.Printf("User: %+v", row)
}
```

#### Update Records

```go
data := map[string]interface{}{
    "email": "jane.smith@newdomain.com",
}

where := map[string]interface{}{
    "id": 1,
}

result, err := db.Update(ctx, 1, "users", data, where)
if err != nil {
    log.Fatal(err)
}
log.Printf("Updated %d records", result.Count)
```

#### Delete Records

```go
where := map[string]interface{}{
    "id": 1,
}

result, err := db.Delete(ctx, 1, "users", where)
if err != nil {
    log.Fatal(err)
}
log.Printf("Deleted %d records", result.Count)
```

## Advanced Operations

### Working with Transactions

PGLiteDB supports full ACID transactions with all PostgreSQL isolation levels:

```go
// Begin a transaction
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSerializable,
})
if err != nil {
    log.Fatal(err)
}

// Perform operations within the transaction
_, err = tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "Alice", "alice@example.com")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

_, err = tx.ExecContext(ctx, "UPDATE users SET email = $1 WHERE name = $2", "alice.new@example.com", "Alice")
if err != nil {
    tx.Rollback()
    log.Fatal(err)
}

// Commit the transaction
err = tx.Commit()
if err != nil {
    log.Fatal(err)
}
```

### Using Savepoints

For nested transaction support, PGLiteDB provides savepoint functionality:

```go
tx, err := db.BeginTx(ctx, nil)
if err != nil {
    log.Fatal(err)
}
defer tx.Rollback()

// Create a savepoint
savepoint, err := tx.Savepoint()
if err != nil {
    log.Fatal(err)
}

// Perform some operations
_, err = tx.ExecContext(ctx, "INSERT INTO users (name, email) VALUES ($1, $2)", "Bob", "bob@example.com")
if err != nil {
    // Rollback to savepoint instead of entire transaction
    tx.RollbackToSavepoint(savepoint)
    log.Printf("Rolled back to savepoint due to error: %v", err)
} else {
    // Release the savepoint
    tx.ReleaseSavepoint(savepoint)
    err = tx.Commit()
    if err != nil {
        log.Fatal(err)
    }
}
```

### Query Planning and Optimization

Use the `Explain` method to understand query execution plans:

```go
plan, err := db.Explain(ctx, "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 25")
if err != nil {
    log.Fatal(err)
}

log.Printf("Query Plan: %+v", plan)
```

## Transaction Management

PGLiteDB implements full MVCC (Multi-Version Concurrency Control) with support for all PostgreSQL isolation levels:

- `READ UNCOMMITTED`
- `READ COMMITTED`
- `REPEATABLE READ`
- `SNAPSHOT ISOLATION`
- `SERIALIZABLE`

### Isolation Level Examples

```go
// Serializable isolation (highest consistency)
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelSerializable,
})

// Repeatable read isolation
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelRepeatableRead,
})

// Read committed isolation (default)
tx, err := db.BeginTx(ctx, &sql.TxOptions{
    Isolation: sql.LevelReadCommitted,
})
```

### Deadlock Detection and Prevention

PGLiteDB includes advanced deadlock detection mechanisms that automatically detect and resolve deadlock situations:

```go
// PGLiteDB will automatically detect and handle deadlocks
// No special handling required in application code
tx1, _ := db.BeginTx(ctx, nil)
tx2, _ := db.BeginTx(ctx, nil)

// Thread 1: Updates user A then tries to update user B
go func() {
    tx1.ExecContext(ctx, "UPDATE users SET email = $1 WHERE id = $2", "newA@example.com", 1)
    tx1.ExecContext(ctx, "UPDATE users SET email = $1 WHERE id = $2", "newB@example.com", 2) // May cause deadlock
}()

// Thread 2: Updates user B then tries to update user A
go func() {
    tx2.ExecContext(ctx, "UPDATE users SET email = $1 WHERE id = $2", "newB2@example.com", 2)
    tx2.ExecContext(ctx, "UPDATE users SET email = $1 WHERE id = $2", "newA2@example.com", 1) // May cause deadlock
}()
```

## Multi-tenancy

PGLiteDB includes built-in multi-tenancy support with isolated data storage:

```go
// Operations for tenant 1
result1, err := db.Select(ctx, 1, "users", options)

// Operations for tenant 2
result2, err := db.Select(ctx, 2, "users", options)

// Each tenant has completely isolated data storage
```

### Tenant Isolation Features

- Physical data separation at the storage layer
- Independent schema management per tenant
- Resource quotas and limits per tenant
- Cross-tenant data access prevention

## Performance Considerations

### Connection Pooling

PGLiteDB includes intelligent connection pooling for optimal performance:

```go
// The embedded client automatically manages connection pooling
// No explicit connection management required
db := client.NewClient("/path/to/database")
```

### Memory Management

PGLiteDB uses advanced memory management techniques:

- Object pooling to reduce allocations
- Memory-comparable encoding for efficient storage
- Automatic resource cleanup and leak detection

### Batch Operations

For bulk operations, use batch processing:

```go
// Batch insert example
batch := make([]map[string]interface{}, 1000)
for i := 0; i < 1000; i++ {
    batch[i] = map[string]interface{}{
        "name":  fmt.Sprintf("User %d", i),
        "email": fmt.Sprintf("user%d@example.com", i),
    }
}

// Execute batch insert (implementation-dependent)
// This is more efficient than individual inserts
```

## Error Handling

PGLiteDB provides comprehensive error handling with detailed error information:

```go
result, err := db.Query(ctx, "INVALID SQL STATEMENT")
if err != nil {
    // Handle different error types
    switch err.(type) {
    case *pq.Error:
        pqErr := err.(*pq.Error)
        log.Printf("PostgreSQL error: %s (code: %s)", pqErr.Message, pqErr.Code)
    default:
        log.Printf("General error: %v", err)
    }
}
```

### Common Error Patterns

```go
// Constraint violation
if pqErr, ok := err.(*pq.Error); ok {
    switch pqErr.Code {
    case "23505": // Unique violation
        log.Println("Unique constraint violation")
    case "23503": // Foreign key violation
        log.Println("Foreign key constraint violation")
    }
}

// Connection errors
if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
    log.Println("Connection timeout")
}
```

## Best Practices

1. **Always use context** for timeout and cancellation control
2. **Handle errors appropriately** with specific error type checking
3. **Use transactions** for atomic operations
4. **Leverage multi-tenancy** for SaaS applications
5. **Monitor performance** using built-in metrics
6. **Close resources** properly to prevent leaks

## Next Steps

- Explore the [API Reference](../api/) for detailed method documentation
- Check out the [Quick Start Guides](./quickstart/) for specific use cases
- Review the [Performance Tuning Guide](./performance_tuning.md) for optimization tips
- Learn about [Multi-tenancy Configuration](./multi_tenancy.md)