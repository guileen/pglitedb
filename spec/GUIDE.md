# PGLiteDB Development Guide (Updated December 2025)

## Table of Contents

1. [Introduction](#introduction)
2. [Project Overview](#project-overview)
3. [Architecture](#architecture)
4. [Development Setup](#development-setup)
5. [Coding Standards](#coding-standards)
6. [Testing](#testing)
7. [Performance Optimization](#performance-optimization)
8. [Documentation](#documentation)
9. [Contributing](#contributing)
10. [Long-term Planning](#long-term-planning)

## Introduction

Welcome to the PGLiteDB Development Guide. This guide provides comprehensive information for developers who want to contribute to or work with PGLiteDB, a high-performance, PostgreSQL-compatible embedded database.

**UPDATE**: PGLiteDB has successfully completed all major architectural improvement phases with comprehensive test coverage enhancements. The project has achieved 100% PostgreSQL regression test compliance (228/228 tests passing) and optimized performance of ~3,100 TPS with ~3.2ms latency. Recent optimizations have achieved significant performance improvements through query plan caching with LRU eviction, parser optimizations with hybrid approach, and enhanced resource management. All parser enhancements have been thoroughly tested and verified.

## Project Overview

PGLiteDB is a cutting-edge embedded database that offers full PostgreSQL wire protocol compatibility while delivering exceptional performance. Built on CockroachDB's Pebble storage engine (an LSM-tree based key-value store), PGLiteDB provides the familiar PostgreSQL interface that developers love with the performance characteristics needed for modern applications.

### Key Features

- **⚡ High Performance** - Optimized for ~3,100 TPS with ~3.2ms latency
- **🔌 True PostgreSQL Compatibility** - Full PostgreSQL wire protocol support
- **🤖 100% AI-Automated Development** - Entire codebase written and optimized by AI agents
- **📦 Embedded & Server Modes** - Run as embedded library or standalone server
- **🌐 Multi-Protocol Access** - PostgreSQL wire protocol, HTTP REST API, and native Go client
- **📋 Full SQL Support** - Standard SQL operations with comprehensive DDL support
- **📈 Advanced Indexing** - Secondary indexes with B-tree and hash implementations
- **🏢 Multi-Tenancy** - Built-in tenant isolation for SaaS applications
- **💾 Robust Storage** - Powered by CockroachDB's Pebble (LSM-tree based key-value store)
- **🧠 Smart Optimizations** - Object pooling, batch operations, connection pooling, and query plan caching
- **🛡️ ACID Compliance** - Full transaction support with MVCC and all isolation levels

## Architecture

### Layered Architecture

PGLiteDB follows a strict layered architecture with clear separation of concerns:

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

### Component-Based Architecture - COMPLETED ✅

The PostgreSQL server has been successfully decomposed into focused, maintainable components:

```
protocol/pgserver/
├── server.go                   # Core server interface and factory (< 100 lines)
├── connection_handler.go       # Connection handling logic (< 300 lines)
├── query_processor.go          # Query processing and execution (< 300 lines)
├── prepared_statement.go       # Prepared statement management (< 200 lines)
├── profiling_service.go        # HTTP profiling endpoints (< 150 lines)
├── buffer_pool.go              # Buffer pool management (< 200 lines)
└── components/                 # Organized by component type
    ├── connection/
    ├── query/
    ├── profiling/
    └── management/
```

### Interface-Driven Design - COMPLETED ✅

All major components now follow interface-driven design principles:

- **Clear Separation of Concerns**: Each component has a single, well-defined responsibility
- **Loose Coupling**: Components interact through well-defined interfaces
- **High Cohesion**: Related functionality is grouped together
- **Testability**: Components can be easily mocked and tested in isolation

### Enhanced Resource Management - COMPLETED ✅

Advanced resource management systems have been implemented:

#### Object Pooling
- **Buffer Pools**: Efficient memory buffer management
- **Iterator Pools**: Reusable iterator objects for database scans
- **Transaction Pools**: Reusable transaction objects
- **Record Pools**: Reusable record objects for query results

#### Leak Detection
- **Automatic Leak Detection**: Tracks resource allocation and release
- **Stack Trace Capture**: Captures allocation stack traces for debugging
- **Periodic Scanning**: Regularly scans for unreleased resources
- **Reporting**: Generates detailed leak reports with actionable information

#### Connection Management
- **Adaptive Pool Sizing**: Dynamically adjusts pool sizes based on workload
- **Health Checking**: Monitors connection health and removes unhealthy connections
- **Timeout Management**: Automatically closes stale connections

## Development Setup

### Prerequisites

- Go 1.21 or higher
- Git
- Make (optional, for convenience)

### Getting Started

1. **Clone the repository**:
   ```bash
   git clone https://github.com/guileen/pglitedb.git
   cd pglitedb
   ```

2. **Install dependencies**:
   ```bash
   go mod tidy
   ```

3. **Run tests**:
   ```bash
   go test ./...
   ```

4. **Build the project**:
   ```bash
   go build ./cmd/server
   ```

### Development Environment

#### Recommended IDE/Editor Setup

- **Visual Studio Code** with Go extension
- **GoLand** for JetBrains users
- **Vim/Neovim** with Go plugins

#### Useful Development Tools

- **Delve**: Debugger for Go applications
- **Goland**: IDE with excellent Go support
- **Golint**: Linter for Go code
- **Go vet**: Static analysis tool
- **Gci**: Import organizer

### Build Commands

```bash
# Build the server
go build -o pglitedb ./cmd/server

# Run tests
go test ./...

# Run tests with coverage
go test -coverprofile=coverage.out ./...
go tool cover -html=coverage.out

# Install the binary
go install ./cmd/server
```

## Coding Standards

### Go Code Style

PGLiteDB follows the standard Go code style with some additional conventions:

#### Naming Conventions

- **Variables**: Use descriptive names, avoid abbreviations
- **Functions**: Use verbs for actions, nouns for getters
- **Interfaces**: Use `er` suffix (e.g., `Reader`, `Writer`)
- **Structs**: Use noun names, capitalize exported fields

#### File Organization

- **Maximum file size**: Keep files under 500 lines when possible
- **Single responsibility**: Each file should have one clear purpose
- **Package organization**: Group related functionality in packages

#### Error Handling

```go
// Good: Wrap errors with context
if err := someOperation(); err != nil {
    return fmt.Errorf("failed to perform operation: %w", err)
}

// Good: Handle errors explicitly
result, err := riskyOperation()
if err != nil {
    // Handle error appropriately
    return nil, err
}
// Use result safely
```

### Component Design Principles

#### Interface Segregation - COMPLETED ✅

Large interfaces have been successfully segregated into focused, cohesive interfaces:

```go
// Good: Segregated interfaces
type Reader interface {
    Read(ctx context.Context, key []byte) ([]byte, error)
}

type Writer interface {
    Write(ctx context.Context, key, value []byte) error
}

type ReadWriter interface {
    Reader
    Writer
}
```

#### Dependency Injection

Use dependency injection for better testability and loose coupling:

```go
// Good: Constructor-based dependency injection
func NewService(dependency Dependency) *Service {
    return &Service{
        dependency: dependency,
    }
}
```

#### Resource Management

Always ensure proper resource cleanup:

```go
// Good: Defer for cleanup
file, err := os.Open("file.txt")
if err != nil {
    return err
}
defer file.Close()

// Good: Check for nil before cleanup
if closer != nil {
    closer.Close()
}
```

## Testing

### Testing Philosophy

PGLiteDB follows a comprehensive testing approach:

1. **Unit Testing**: Test individual components in isolation
2. **Integration Testing**: Test component interactions
3. **Regression Testing**: Maintain 100% PostgreSQL compatibility
4. **Performance Testing**: Ensure performance targets are met
5. **Concurrency Testing**: Validate thread safety and race conditions

### Test Structure

Tests should follow the AAA pattern (Arrange, Act, Assert):

```go
func TestSomething(t *testing.T) {
    // Arrange
    mock := NewMock()
    sut := NewService(mock)
    
    // Act
    result, err := sut.DoSomething()
    
    // Assert
    assert.NoError(t, err)
    assert.NotNil(t, result)
}
```

### Test Coverage Requirements

- **Core Packages**: >95% test coverage
- **Interface Coverage**: >95% interface coverage in tests
- **Regression Tests**: 100% PostgreSQL regress test compliance (228/228 tests passing)

### Testing Best Practices

#### Mocking and Stubbing

Use mocking for external dependencies:

```go
// Good: Interface-based mocking
type MockStorage struct {
    // Implement interface methods
}

func (m *MockStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
    // Return test data
}
```

#### Table-Driven Tests

Use table-driven tests for multiple test cases:

```go
func TestParseQuery(t *testing.T) {
    tests := []struct {
        name     string
        input    string
        expected *ParsedQuery
        wantErr  bool
    }{
        {
            name:    "simple select",
            input:   "SELECT * FROM users",
            expected: &ParsedQuery{Type: SelectStatement},
            wantErr: false,
        },
        // More test cases...
    }
    
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            result, err := ParseQuery(tt.input)
            if tt.wantErr {
                assert.Error(t, err)
            } else {
                assert.NoError(t, err)
                assert.Equal(t, tt.expected, result)
            }
        })
    }
}
```

### Performance Testing

Performance tests should validate:

- **Throughput**: Transactions per second (TPS)
- **Latency**: Average response time
- **Memory Usage**: Memory allocation patterns
- **Scalability**: Performance under increasing load

```go
func BenchmarkStorageEngine(b *testing.B) {
    // Setup
    engine := setupTestEngine()
    defer engine.Close()
    
    b.ResetTimer()
    
    // Run benchmark
    for i := 0; i < b.N; i++ {
        _, _ = engine.ExecuteQuery(testQuery)
    }
}
```

## Performance Optimization

### Optimization Strategies - COMPLETED ✅

#### Query Plan Caching - COMPLETED ✅

Query plan caching has been implemented with LRU eviction:

- **3x Performance Improvements**: For repeated queries
- **Reduced CPU Usage**: Eliminates repeated parsing and planning
- **Improved Response Times**: Faster execution for common queries

#### Object Pooling - COMPLETED ✅

Extensive object pooling reduces garbage collection overhead:

- **Up to 90% Reduction**: In memory allocations for key operations
- **Improved Throughput**: Higher transaction processing rates
- **Reduced Latency**: Lower response times due to reduced GC pauses

#### Memory Management - COMPLETED ✅

Advanced memory management techniques optimize performance:

- **Zero-Allocation Encoding**: Encodes data without additional memory allocations
- **Memory-Comparable Encoding**: Preserves sort order for efficient range scans
- **Batch Operations**: Combines multiple operations for reduced I/O overhead

### Profiling and Benchmarking

Use Go's built-in profiling tools:

```bash
# CPU profiling
go test -bench=. -cpuprofile cpu.prof
go tool pprof cpu.prof

# Memory profiling
go test -bench=. -memprofile mem.prof
go tool pprof mem.prof

# Block profiling
go test -bench=. -blockprofile block.prof
go tool pprof block.prof
```

### Performance Monitoring

Implement performance monitoring in code:

```go
// Good: Performance monitoring
func (s *Service) ProcessQuery(ctx context.Context, query string) (*Result, error) {
    start := time.Now()
    defer func() {
        duration := time.Since(start)
        metrics.RecordQueryDuration(duration)
    }()
    
    // Process query...
}
```

## Documentation

### Documentation Standards

All public APIs should be documented with Godoc comments:

```go
// Good: Godoc documentation
// UserService provides user management functionality.
type UserService struct {
    // ...
}

// CreateUser creates a new user with the given details.
// Returns the created user and any error that occurred.
func (s *UserService) CreateUser(details UserDetails) (*User, error) {
    // Implementation...
}
```

### Documentation Structure

Documentation should be organized as follows:

1. **API Reference**: Detailed documentation of all public APIs
2. **User Guides**: Step-by-step guides for different use cases
3. **Examples**: Runnable examples demonstrating features
4. **Architecture**: High-level architectural overview
5. **Performance**: Performance characteristics and optimization guides

### Example Documentation

Include runnable examples in documentation:

```go
// Example of using the client
func ExampleClient_Insert() {
    client := NewClient("/tmp/db")
    ctx := context.Background()
    
    data := map[string]interface{}{
        "name": "John Doe",
        "email": "john@example.com",
    }
    
    result, err := client.Insert(ctx, 1, "users", data)
    if err != nil {
        log.Fatal(err)
    }
    
    fmt.Printf("Inserted %d rows", result.Count)
    // Output: Inserted 1 rows
}
```

## Contributing

### Contribution Process

1. **Fork the repository**
2. **Create a feature branch**
3. **Make your changes**
4. **Add tests if applicable**
5. **Submit a pull request**

### Code Review Process

All contributions undergo code review:

1. **Automated Checks**: CI/CD pipeline runs tests and linting
2. **Peer Review**: At least one reviewer required
3. **Performance Validation**: Performance tests for optimization changes
4. **Compatibility Testing**: PostgreSQL regression tests must pass

### Pull Request Guidelines

Pull requests should include:

1. **Clear Description**: Explain what the PR does and why
2. **Tests**: Include tests for new functionality
3. **Documentation**: Update documentation if needed
4. **Performance Impact**: Note any performance implications

### Issue Reporting

When reporting issues, include:

1. **Clear Description**: What went wrong and expected behavior
2. **Steps to Reproduce**: Minimal steps to reproduce the issue
3. **Environment**: Go version, OS, and other relevant details
4. **Logs/Errors**: Any relevant error messages or logs

### Community Guidelines

Follow these guidelines for a positive community experience:

1. **Be Respectful**: Treat all contributors with respect
2. **Be Constructive**: Provide constructive feedback
3. **Be Helpful**: Help others when possible
4. **Be Patient**: Give reviewers time to respond

---

*Last updated: December 2025*