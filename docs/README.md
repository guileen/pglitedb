# PGLiteDB Documentation

Welcome to the PGLiteDB documentation. This documentation provides comprehensive information about using and understanding PGLiteDB.

## Table of Contents

1. [Getting Started](#getting-started)
2. [Architecture](#architecture)
3. [Components](#components)
4. [Performance](#performance)
5. [Resource Management](#resource-management)
6. [API Reference](#api-reference)
7. [Examples](#examples)
8. [Troubleshooting](#troubleshooting)

## Getting Started

### Installation
- [Quick Start Guide](guides/quickstart.md) - Step-by-step guide to get started with PGLiteDB
- [Installation Instructions](../README.md#installation) - How to install PGLiteDB

### Basic Usage
- [Embedded Usage Guide](guides/embedded_usage.md) - How to use PGLiteDB as an embedded database
- [Server Mode](../README.md#starting-the-server) - Running PGLiteDB as a server
- [Client Connections](../README.md#using-with-postgresql-clients) - Connecting with PostgreSQL clients

## Architecture

- [Architecture Overview](architecture.md) - Detailed architecture documentation
- [Component Structure](component_structure.md) - Detailed component structure and interactions
- [Layer Responsibilities](../README.md#layer-responsibilities) - Understanding the different layers

## Components

### Protocol Layer
- [PostgreSQL Wire Protocol](../README.md#postgresql-wire-protocol-server-pgserver) - PostgreSQL protocol implementation
- [HTTP REST API](../README.md#http-rest-api) - REST API implementation

### Executor Layer
- [SQL Parser](../README.md#sql-parser) - SQL parsing functionality
- [Query Planner](../README.md#query-planner) - Query planning and optimization
- [Query Executor](../README.md#query-executor) - Query execution

### Engine Layer
- [Storage Engine](../README.md#storage-engine) - Core storage engine implementation
- [Table Management](../README.md#table-manager) - Table management functionality
- [Index Management](../README.md#index-manager) - Index management functionality

### Storage Layer
- [Pebble Storage](../README.md#storage-layer) - Pebble storage engine integration
- [Data Encoding](../README.md#codec) - Data encoding and decoding

## Performance

- [Performance Optimizations](performance_optimizations.md) - Detailed performance optimization techniques
- [Benchmark Results](../README.md#-performance-benchmarks) - Current performance benchmarks
- [Query Plan Caching](performance_optimizations.md#query-plan-caching) - Query plan caching implementation
- [Object Pooling](performance_optimizations.md#object-pooling) - Object pooling strategies
- [Connection Pooling](performance_optimizations.md#connection-pooling) - Connection pooling implementation

## Resource Management

- [Resource Management Overview](resource_management.md) - Comprehensive resource management strategies
- [Object Pooling](resource_management.md#object-pooling) - Object pooling implementation details
- [Connection Management](resource_management.md#connection-management) - Connection management strategies
- [Leak Detection](resource_management.md#leak-detection) - Resource leak detection and prevention
- [Memory Management](resource_management.md#memory-management) - Memory management techniques
- [Resource Monitoring](resource_management.md#resource-monitoring) - Resource monitoring and metrics

## API Reference

- [Client API](api/reference.md) - Complete API reference for the embedded client
- [Server API](api/server.md) - Server API documentation
- [REST API](api/rest.md) - HTTP REST API reference

## Examples

- [Basic Operations](guides/interactive_examples.md#basic-operations) - CRUD operations examples
- [Advanced Querying](guides/interactive_examples.md#advanced-querying) - Complex queries and joins
- [Transactions](guides/interactive_examples.md#transactions) - Transaction management examples
- [Multi-tenancy](guides/interactive_examples.md#multi-tenancy) - Multi-tenancy implementation examples
- [Performance Testing](guides/interactive_examples.md#performance-testing) - Performance benchmarking examples

## Troubleshooting

- [Common Issues](troubleshooting/common_issues.md) - Solutions to common problems
- [Performance Issues](troubleshooting/performance_issues.md) - Diagnosing and resolving performance problems
- [Connection Issues](troubleshooting/connection_issues.md) - Troubleshooting connection problems
- [Error Messages](troubleshooting/error_messages.md) - Understanding error messages

---

*Last updated: December 2025*