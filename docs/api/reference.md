# PGLiteDB API Reference

This document provides detailed reference documentation for the PGLiteDB API, covering all public methods and types available for use in your applications.

## Table of Contents

1. [Client Package](#client-package)
2. [Types Package](#types-package)
3. [Core Methods](#core-methods)
4. [Transaction Methods](#transaction-methods)
5. [Utility Methods](#utility-methods)

## Client Package

The `client` package provides the main interface for interacting with PGLiteDB.

### Import Statement

```go
import "github.com/guileen/pglitedb/client"
```

### Types

#### Client

The main client struct for interacting with PGLiteDB.

```go
type Client struct {
    // Contains filtered or unexported fields
}
```

### Functions

#### NewClient

```go
func NewClient(dbPath string) *Client
```

NewClient creates a new embedded client instance connected to the database at the specified path.

**Parameters:**
- `dbPath` (string): Path to the database directory

**Returns:**
- `*Client`: A new client instance

**Example:**
```go
db := client.NewClient("/path/to/database")
```

#### NewClientWithExecutor

```go
func NewClientWithExecutor(exec *sql.Executor, planner *sql.Planner) *Client
```

NewClientWithExecutor creates a new client with custom executor and planner.

**Parameters:**
- `exec` (*sql.Executor): Custom SQL executor
- `planner` (*sql.Planner): Custom SQL planner

**Returns:**
- `*Client`: A new client instance

### Methods

#### Query

```go
func (c *Client) Query(ctx context.Context, query interface{}) (*types.QueryResult, error)
```

Query executes a raw SQL query and returns the result.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `query` (interface{}): SQL query string to execute

**Returns:**
- `*types.QueryResult`: Query result containing rows and metadata
- `error`: Error if the query fails

**Example:**
```go
result, err := db.Query(ctx, "SELECT * FROM users WHERE age > 25")
```

#### Explain

```go
func (c *Client) Explain(ctx context.Context, query interface{}) (interface{}, error)
```

Explain generates an execution plan for a query without executing it.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `query` (interface{}): SQL query string to explain

**Returns:**
- `interface{}`: Execution plan information
- `error`: Error if plan generation fails

**Example:**
```go
plan, err := db.Explain(ctx, "SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id")
```

#### Insert

```go
func (c *Client) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.QueryResult, error)
```

Insert inserts a new record into the specified table.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `tenantID` (int64): Tenant identifier for multi-tenancy
- `tableName` (string): Name of the table to insert into
- `data` (map[string]interface{}): Map of column names to values

**Returns:**
- `*types.QueryResult`: Result containing insert metadata
- `error`: Error if insertion fails

**Example:**
```go
data := map[string]interface{}{
    "name": "John Doe",
    "email": "john@example.com",
    "age": 30,
}
result, err := db.Insert(ctx, 1, "users", data)
```

#### Select

```go
func (c *Client) Select(ctx context.Context, tenantID int64, tableName string, options *types.QueryOptions) (*types.QueryResult, error)
```

Select retrieves records from the specified table based on query options.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `tenantID` (int64): Tenant identifier for multi-tenancy
- `tableName` (string): Name of the table to query
- `options` (*types.QueryOptions): Query options including filters, ordering, etc.

**Returns:**
- `*types.QueryResult`: Query result containing matching rows
- `error`: Error if query fails

**Example:**
```go
options := &types.QueryOptions{
    Where: map[string]interface{}{
        "age": map[string]interface{}{"$gt": 25},
    },
    OrderBy: []string{"name ASC"},
    Limit: intPtr(10),
}
result, err := db.Select(ctx, 1, "users", options)
```

#### Update

```go
func (c *Client) Update(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}, where map[string]interface{}) (*types.QueryResult, error)
```

Update modifies existing records in the specified table.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `tenantID` (int64): Tenant identifier for multi-tenancy
- `tableName` (string): Name of the table to update
- `data` (map[string]interface{}): Map of column names to new values
- `where` (map[string]interface{}): Conditions to identify records to update

**Returns:**
- `*types.QueryResult`: Result containing update metadata
- `error`: Error if update fails

**Example:**
```go
data := map[string]interface{}{
    "email": "newemail@example.com",
}
where := map[string]interface{}{
    "id": 123,
}
result, err := db.Update(ctx, 1, "users", data, where)
```

#### Delete

```go
func (c *Client) Delete(ctx context.Context, tenantID int64, tableName string, where map[string]interface{}) (*types.QueryResult, error)
```

Delete removes records from the specified table.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `tenantID` (int64): Tenant identifier for multi-tenancy
- `tableName` (string): Name of the table to delete from
- `where` (map[string]interface{}): Conditions to identify records to delete

**Returns:**
- `*types.QueryResult`: Result containing deletion metadata
- `error`: Error if deletion fails

**Example:**
```go
where := map[string]interface{}{
    "id": 123,
}
result, err := db.Delete(ctx, 1, "users", where)
```

## Types Package

The `types` package contains common types used throughout PGLiteDB.

### QueryOptions

QueryOptions represents query options for SELECT operations.

```go
type QueryOptions struct {
    Columns []string               `json:"columns,omitempty"`
    Where   map[string]interface{} `json:"where,omitempty"`
    OrderBy []string               `json:"order_by,omitempty"`
    Limit   *int                   `json:"limit,omitempty"`
    Offset  *int                   `json:"offset,omitempty"`
    Include []string               `json:"include,omitempty"`
    Count   bool                   `json:"count,omitempty"`
    Head    bool                   `json:"head,omitempty"`
    Debug   bool                   `json:"debug,omitempty"`
}
```

### QueryResult

QueryResult represents the result of a query operation.

```go
type QueryResult struct {
    Rows    [][]interface{} `json:"rows"`
    Columns []ColumnInfo    `json:"columns"`
    Count   int64           `json:"count,omitempty"`
    HasMore bool            `json:"has_more,omitempty"`
    
    // Extended fields for internal use
    Records      interface{}   `json:"-"`
    Duration     time.Duration `json:"-"` 
    Query        string        `json:"-"`
    Params       []interface{} `json:"-"`
    Debug        interface{}   `json:"-"`
    Limit        *int          `json:"-"`
    Offset       *int          `json:"-"`
    LastInsertID int64         `json:"-"`
}
```

### ColumnInfo

ColumnInfo represents metadata about a column in a query result.

```go
type ColumnInfo struct {
    Name string
    Type ColumnType
}
```

### ColumnType

ColumnType represents the data type of a column.

```go
type ColumnType int

const (
    ColumnTypeUnknown ColumnType = iota
    ColumnTypeBoolean
    ColumnTypeInteger
    ColumnTypeBigInteger
    ColumnTypeFloat
    ColumnTypeDouble
    ColumnTypeString
    ColumnTypeText
    ColumnTypeBinary
    ColumnTypeDate
    ColumnTypeTime
    ColumnTypeTimestamp
    ColumnTypeJSON
)
```

## Core Methods

### BeginTx

```go
func (c *Client) BeginTx(ctx context.Context, opts *sql.TxOptions) (*Tx, error)
```

BeginTx starts a new transaction with the specified options.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `opts` (*sql.TxOptions): Transaction options including isolation level

**Returns:**
- `*Tx`: Transaction object
- `error`: Error if transaction cannot be started

### Close

```go
func (c *Client) Close() error
```

Close closes the database connection and releases resources.

**Returns:**
- `error`: Error if closing fails

## Transaction Methods

### Tx (Transaction)

The Tx type represents a database transaction.

```go
type Tx struct {
    // Contains filtered or unexported fields
}
```

#### Commit

```go
func (tx *Tx) Commit() error
```

Commit commits the transaction.

**Returns:**
- `error`: Error if commit fails

#### Rollback

```go
func (tx *Tx) Rollback() error
```

Rollback rolls back the transaction.

**Returns:**
- `error`: Error if rollback fails

#### Savepoint

```go
func (tx *Tx) Savepoint() (string, error)
```

Savepoint creates a new savepoint within the transaction.

**Returns:**
- `string`: Savepoint identifier
- `error`: Error if savepoint creation fails

#### RollbackToSavepoint

```go
func (tx *Tx) RollbackToSavepoint(name string) error
```

RollbackToSavepoint rolls back to the specified savepoint.

**Parameters:**
- `name` (string): Savepoint identifier

**Returns:**
- `error`: Error if rollback fails

#### ReleaseSavepoint

```go
func (tx *Tx) ReleaseSavepoint(name string) error
```

ReleaseSavepoint releases the specified savepoint.

**Parameters:**
- `name` (string): Savepoint identifier

**Returns:**
- `error`: Error if release fails

#### ExecContext

```go
func (tx *Tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
```

ExecContext executes a query that doesn't return rows within the transaction.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `query` (string): SQL query to execute
- `args` (...interface{}): Query arguments

**Returns:**
- `sql.Result`: Result of the execution
- `error`: Error if execution fails

#### QueryContext

```go
func (tx *Tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
```

QueryContext executes a query that returns rows within the transaction.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `query` (string): SQL query to execute
- `args` (...interface{}): Query arguments

**Returns:**
- `*sql.Rows`: Query result rows
- `error`: Error if query fails

#### QueryRowContext

```go
func (tx *Tx) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
```

QueryRowContext executes a query that returns a single row within the transaction.

**Parameters:**
- `ctx` (context.Context): Context for cancellation and timeouts
- `query` (string): SQL query to execute
- `args` (...interface{}): Query arguments

**Returns:**
- `*sql.Row`: Single result row

## Utility Methods

### Helper Functions

#### intPtr

```go
func intPtr(i int) *int
```

intPtr creates a pointer to an integer value.

**Parameters:**
- `i` (int): Integer value

**Returns:**
- `*int`: Pointer to the integer

### Constants

PGLiteDB defines several constants for configuration and behavior:

```go
const (
    DefaultPort = 5432
    DefaultHTTPPort = 8080
    DefaultMaxConnections = 100
)
```

## Error Handling

PGLiteDB uses standard Go error handling patterns. Common error types include:

- `*pq.Error`: PostgreSQL-specific errors
- `context.DeadlineExceeded`: Timeout errors
- `context.Canceled`: Cancellation errors
- Standard Go errors for general failures

### Error Checking Example

```go
result, err := db.Query(ctx, "INVALID SQL")
if err != nil {
    switch err {
    case context.DeadlineExceeded:
        // Handle timeout
    case context.Canceled:
        // Handle cancellation
    default:
        // Handle other errors
        if pqErr, ok := err.(*pq.Error); ok {
            // Handle PostgreSQL-specific errors
            fmt.Printf("PostgreSQL error %s: %s\n", pqErr.Code, pqErr.Message)
        }
    }
}
```

## Best Practices

1. **Always use context** for timeout and cancellation control
2. **Close resources** properly to prevent leaks
3. **Handle errors** appropriately with specific error type checking
4. **Use transactions** for atomic operations
5. **Validate inputs** before passing to database methods
6. **Use connection pooling** for high-concurrency applications
7. **Monitor performance** using built-in metrics

## See Also

- [Embedded Usage Guide](./embedded_usage.md)
- [Quick Start Guides](./quickstart.md)
- [Interactive Examples](./interactive_examples.md)