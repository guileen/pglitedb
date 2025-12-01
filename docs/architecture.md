# PGLiteDB Architecture Documentation

This document provides detailed information about the PGLiteDB architecture, component structure, and design principles.

## Table of Contents

1. [Overview](#overview)
2. [Component Architecture](#component-architecture)
3. [Protocol Layer](#protocol-layer)
4. [Executor Layer](#executor-layer)
5. [Engine Layer](#engine-layer)
6. [Storage Layer](#storage-layer)
7. [Resource Management](#resource-management)
8. [Performance Optimizations](#performance-optimizations)

## Overview

PGLiteDB is a high-performance, PostgreSQL-compatible embedded database built on CockroachDB's Pebble storage engine. The architecture follows a layered approach with clear separation of concerns and well-defined interfaces between components.

## Component Architecture

The PGLiteDB architecture consists of four main layers:

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

## Protocol Layer

The protocol layer handles client connections and communication protocols.

### PostgreSQL Wire Protocol Server (pgserver)

The PostgreSQL wire protocol server implements the PostgreSQL frontend/backend protocol, allowing standard PostgreSQL clients to connect to PGLiteDB.

Key components:
- **Connection Management**: Handles client connections, authentication, and session management
- **Query Processing**: Parses and executes SQL queries received from clients
- **Prepared Statements**: Manages prepared statements and portals for efficient query execution
- **Buffer Pool Management**: Manages memory buffers for network I/O operations

### HTTP REST API (api/rest)

The HTTP REST API provides a web-based interface for database operations, making it easy to integrate with web applications.

## Executor Layer

The executor layer is responsible for SQL parsing, query planning, and execution.

### SQL Parser (protocol/sql/parser)

The SQL parser component parses SQL queries and converts them into an abstract syntax tree (AST). It supports both a simple parser for basic queries and a full parser based on libpg_query for complex queries.

### Query Planner (protocol/sql/planner)

The query planner creates execution plans from parsed queries. It includes:
- **Plan Caching**: Caches query plans to avoid repeated planning for identical queries
- **Query Optimization**: Applies optimization rules to improve query performance
- **Cost-Based Optimization**: Uses statistics to estimate query costs and choose optimal plans

### Executor (protocol/sql/executor)

The executor component executes query plans and returns results. It handles:
- **DML Operations**: INSERT, UPDATE, DELETE operations
- **DDL Operations**: CREATE, ALTER, DROP operations
- **Transaction Management**: BEGIN, COMMIT, ROLLBACK operations
- **Query Execution**: SELECT operations with filtering, sorting, and aggregation

## Engine Layer

The engine layer provides the core database functionality.

### Storage Engine (engine/pebble)

The storage engine is built on CockroachDB's Pebble key-value store. Key features include:
- **ACID Transactions**: Full ACID compliance with MVCC support
- **Multi-Tenancy**: Isolated data storage for different tenants
- **Index Support**: Secondary indexes with B-tree and hash implementations
- **Resource Management**: Connection pooling, object pooling, and memory management

### Table Manager (engine/table)

The table manager handles table definitions, schema management, and metadata storage.

### Index Manager (engine/index)

The index manager provides index creation, maintenance, and query optimization support.

## Storage Layer

The storage layer provides the underlying key-value storage functionality.

### Pebble KV Store (storage/pebble)

Pebble is a high-performance key-value store based on RocksDB/RocksDB. It provides:
- **LSM-Tree Storage**: Log-Structured Merge-Tree for efficient write operations
- **Snapshots**: Point-in-time views of the database
- **Iterators**: Efficient key-value iteration
- **Batch Operations**: Atomic batch operations for performance

### Codec (codec/)

The codec component provides data encoding and decoding functionality for efficient storage and retrieval.

## Resource Management

PGLiteDB implements comprehensive resource management to optimize performance and prevent resource leaks.

### Object Pooling

Object pooling reduces memory allocations and garbage collection overhead:
- **Buffer Pools**: Reusable byte buffers for network and file I/O
- **Iterator Pools**: Reusable iterator objects for database scans
- **Transaction Pools**: Reusable transaction objects
- **Record Pools**: Reusable record objects for query results

### Connection Pooling

Connection pooling manages database connections efficiently:
- **Adaptive Pool Sizing**: Dynamically adjusts pool size based on workload
- **Health Checking**: Monitors connection health and removes unhealthy connections
- **Timeout Management**: Automatically closes stale connections

### Memory Management

Memory management optimizes memory usage and reduces allocations:
- **Zero-Allocation Encoding**: Encodes data without additional memory allocations
- **Memory-Comparable Encoding**: Encodes data in a way that preserves sort order
- **Resource Leak Detection**: Detects and prevents resource leaks

## Performance Optimizations

PGLiteDB implements several performance optimizations to deliver exceptional performance:

### Query Plan Caching

Query plan caching stores execution plans for repeated queries, eliminating the need to re-parse and re-plan identical queries.

### Object Pooling

Object pooling reduces garbage collection overhead by reusing frequently allocated objects.

### Batch Operations

Batch operations combine multiple operations into a single atomic operation, reducing I/O overhead.

### Memory Management

Efficient memory management techniques reduce allocations and improve cache locality.

### Concurrency Optimization

Concurrency optimizations improve performance in multi-threaded environments:
- **Fine-Grained Locking**: Reduces lock contention
- **Read-Optimized Data Structures**: Optimizes for read-heavy workloads
- **Parallel Processing**: Utilizes multiple CPU cores for parallel execution

---

*Last updated: December 2025*