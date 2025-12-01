# PGLiteDB Component Structure

This document describes the detailed component structure of PGLiteDB, showing how different modules interact with each other.

## Table of Contents

1. [Core Components](#core-components)
2. [Protocol Layer Components](#protocol-layer-components)
3. [Executor Layer Components](#executor-layer-components)
4. [Engine Layer Components](#engine-layer-components)
5. [Storage Layer Components](#storage-layer-components)
6. [Utility Components](#utility-components)
7. [Resource Management Components](#resource-management-components)

## Core Components

### Client Library (client/)
The client library provides a Go API for embedding PGLiteDB in applications.

Key files:
- `client.go` - Main client interface
- `config.go` - Client configuration

### Command Line Tools (cmd/)
Command line tools for running PGLiteDB as a server.

Key directories:
- `cmd/server/` - Main server executable
- `cmd/profiling/` - Profiling tools

## Protocol Layer Components

### PostgreSQL Wire Protocol Server (protocol/pgserver/)

The PostgreSQL wire protocol server implements the PostgreSQL frontend/backend protocol.

#### Main Components

- `server.go` - Main server implementation
- `server_config.go` - Server configuration management
- `server_profiling.go` - HTTP profiling endpoints

#### Component Structure

```
protocol/pgserver/
├── components/                 # Component implementations
│   ├── buffer/
│   │   └── pool_manager.go     # Buffer pool management
│   ├── config/
│   │   └── manager.go          # Configuration management
│   ├── connection/
│   │   ├── manager.go          # Connection management
│   │   ├── pool_manager.go     # Connection pool management
│   │   ├── acceptor.go         # Connection acceptance interface
│   │   └── acceptor_impl.go    # Connection acceptance implementation
│   ├── listener/
│   │   ├── manager.go          # Listener management interface
│   │   └── manager_impl.go     # Listener management implementation
│   ├── management/
│   │   └── statement_manager.go # Prepared statement management
│   ├── parameter/
│   │   ├── binder.go           # Parameter binding
│   │   └── utils.go            # Parameter utilities
│   ├── profiling/
│   │   └── service.go          # Profiling service
│   ├── query/
│   │   └── processor.go        # Query processing
│   └── server/
│       └── management.go       # Server management
├── config/                     # Configuration definitions
│   └── config.go              # Server configuration
├── interfaces/                 # Component interfaces
│   ├── buffer_pool_management.go
│   ├── config_management.go
│   ├── connection_acceptance.go
│   ├── connection_handler.go
│   ├── connection_management.go
│   ├── connection_pool_management.go
│   ├── listener_management.go
│   ├── prepared_statement_manager.go
│   ├── profiling_service.go
│   ├── query_processor.go
│   └── server_management.go
├── internal/                   # Internal implementations
│   ├── components/
│   │   ├── connection_handler_impl.go     # Connection handler implementation
│   │   ├── prepared_statement_manager_impl.go # Prepared statement manager implementation
│   │   ├── query_processor_impl.go        # Query processor implementation
│   │   └── profiling_service_impl.go      # Profiling service implementation
│   └── server/
│       ├── lifecycle.go        # Server lifecycle management
│       ├── network.go          # Network operations
│       └── profiling.go        # Profiling operations
└── parameter_binder.go        # Parameter binding utilities
```

### HTTP REST API (protocol/api/)

The HTTP REST API provides a web-based interface for database operations.

Key files:
- `rest.go` - Main REST handler
- `handlers.go` - Individual API endpoint handlers

## Executor Layer Components

### SQL Parser (protocol/sql/parser/)

The SQL parser component handles parsing of SQL queries.

Key files:
- `parser.go` - Main parser interface
- `pg_parser.go` - PostgreSQL parser implementation
- `pg_parser_hybrid.go` - Hybrid parser with caching
- `simple_parser.go` - Simple parser for basic queries

### Query Planner (protocol/sql/planner/)

The query planner creates execution plans from parsed queries.

Key files:
- `planner.go` - Main planner implementation
- `optimizer.go` - Query optimization
- `plan_cache.go` - Query plan caching

### Query Executor (protocol/sql/executor/)

The query executor executes query plans and returns results.

Key files:
- `executor.go` - Main executor implementation
- `executor_dml.go` - DML operation execution
- `executor_ddl.go` - DDL operation execution
- `executor_transaction.go` - Transaction management

## Engine Layer Components

### Storage Engine (engine/pebble/)

The storage engine provides the core database functionality built on Pebble.

#### Core Components

- `engine.go` - Main engine interface
- `engine_core.go` - Core engine implementation
- `transaction_manager.go` - Transaction management
- `storage_manager.go` - Storage management

#### Subsystems

```
engine/pebble/
├── operations/                 # Database operations
│   ├── batch/                  # Batch operations
│   ├── modify/                 # Modify operations
│   ├── query/                  # Query operations
│   └── scan/                   # Scan operations
├── indexes/                    # Index management
├── resources/                  # Resource management
│   ├── pools/                  # Object pools
│   ├── leak/                  # Leak detection
│   └── metrics/               # Resource metrics
├── utils/                      # Utility functions
└── config/                     # Engine configuration
```

### Table Manager (engine/table/)

The table manager handles table definitions and schema management.

Key files:
- `manager.go` - Main table manager
- `definition.go` - Table definition structures

### Engine Types (engine/types/)

Common type definitions used throughout the engine layer.

Key files:
- `types.go` - Core engine types
- `transaction.go` - Transaction types
- `iterator.go` - Iterator types

## Storage Layer Components

### Pebble Storage (storage/)

The storage layer provides the underlying key-value storage functionality.

Key files:
- `storage.go` - Main storage interface
- `pebble.go` - Pebble storage implementation
- `mvcc.go` - Multi-version concurrency control

### Codec (codec/)

Data encoding and decoding functionality.

Key files:
- `codec.go` - Main codec interface
- `memcomparable.go` - Memory-comparable encoding

## Utility Components

### Logger (logger/)

Logging utilities for debugging and monitoring.

Key files:
- `logger.go` - Main logger implementation

### Pool (pool/)

General-purpose object pooling utilities.

Key files:
- `pool.go` - Main pool interface
- `buffer_pool.go` - Buffer pool implementation
- `slice_pool.go` - Slice pool implementation

### Network (network/)

Network utilities for connection management.

Key files:
- `connection_pool.go` - Connection pooling
- `connection_factory.go` - Connection factory

## Resource Management Components

### Resource Pools (engine/pebble/resources/pools/)

Specialized object pools for database resources.

Key files:
- `manager.go` - Pool manager
- `buffer.go` - Buffer pools
- `iterator.go` - Iterator pools
- `transaction.go` - Transaction pools

### Leak Detection (engine/pebble/leak_detection/)

Resource leak detection and prevention.

Key files:
- `leak_detector.go` - Main leak detector
- `tracked_resource.go` - Tracked resource implementation
- `metrics.go` - Leak detection metrics

### Metrics Collection (engine/pebble/resources/metrics/)

Resource usage metrics collection.

Key files:
- `collector.go` - Metrics collector
- `metrics.go` - Metrics definitions

---

*Last updated: December 2025*