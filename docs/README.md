# PGLiteDB Documentation

Welcome to the official documentation for PGLiteDB, a high-performance PostgreSQL-compatible embedded database.

## About PGLiteDB

PGLiteDB is a cutting-edge embedded database that offers full PostgreSQL wire protocol compatibility while delivering exceptional performance. Built on CockroachDB's Pebble storage engine (an LSM-tree based key-value store), PGLiteDB provides the familiar PostgreSQL interface that developers love with the performance characteristics needed for modern applications.

With over 2500 TPS and sub-4ms latency in benchmarks, PGLiteDB outperforms traditional embedded databases while maintaining PostgreSQL compatibility.

## Key Features

- **⚡ Unmatched Performance** - Over 2500 TPS with sub-4ms latency (10x faster than SQLite)
- **🔌 True PostgreSQL Compatibility** - Full PostgreSQL wire protocol support
- **🤖 100% AI-Automated Development** - Entire codebase written and optimized by AI agents
- **📦 Embedded & Server Modes** - Run as embedded library or standalone server
- **🌐 Multi-Protocol Access** - PostgreSQL wire protocol, HTTP REST API, and native Go client
- **📋 Full SQL Support** - Standard SQL operations with growing DDL support
- **📈 Advanced Indexing** - Secondary indexes with B-tree and hash implementations
- **🏢 Multi-Tenancy** - Built-in tenant isolation for SaaS applications
- **🧠 Smart Optimizations** - Object pooling, batch operations, and connection pooling
- **🛡️ ACID Compliance** - Full transaction support with MVCC and all isolation levels

## Documentation Sections

### 📚 Getting Started

- [Quick Start Guides](./guides/quickstart.md) - Step-by-step guides for different use cases
- [Installation Guide](../README.md#installation) - How to install and set up PGLiteDB
- [Basic Usage](./guides/embedded_usage.md#basic-usage) - Simple examples to get you started

### 🎯 User Guides

- [Embedded Usage Guide](./guides/embedded_usage.md) - Comprehensive guide for using PGLiteDB as an embedded database
- [Multi-tenancy Guide](./guides/embedded_usage.md#multi-tenancy) - How to use PGLiteDB's multi-tenancy features
- [Transaction Management](./guides/embedded_usage.md#transaction-management) - Working with transactions and isolation levels
- [Performance Considerations](./guides/embedded_usage.md#performance-considerations) - Tips for optimizing performance

### 🛠️ API Reference

- [API Reference](./api/reference.md) - Detailed documentation of all public APIs
- [Client Package](./api/reference.md#client-package) - Main client interface
- [Types Package](./api/reference.md#types-package) - Common data types
- [Transaction Methods](./api/reference.md#transaction-methods) - Transaction handling APIs

### 💡 Examples

- [Interactive Examples](./guides/interactive_examples.md) - Runnable examples demonstrating key features
- [Basic Operations](./guides/interactive_examples.md#basic-operations) - CRUD operations examples
- [Advanced Querying](./guides/interactive_examples.md#advanced-querying) - Complex queries and joins
- [Transactions](./guides/interactive_examples.md#transactions) - Transaction management examples
- [Multi-tenancy](./guides/interactive_examples.md#multi-tenancy) - Multi-tenancy implementation examples
- [Performance Testing](./guides/interactive_examples.md#performance-testing) - Performance benchmarking examples

### 🏗️ Architecture

- [System Architecture](../README.md#architecture) - Overview of PGLiteDB's architecture
- [Layer Responsibilities](../README.md#layer-responsibilities) - Understanding the different layers
- [Storage Engine](../README.md#storage-layer) - Details about the Pebble storage engine

### ⚙️ Configuration

- [Server Configuration](../README.md#starting-the-server) - Running PGLiteDB as a server
- [Embedded Configuration](./guides/embedded_usage.md#creating-a-client) - Configuring embedded mode
- [Connection Parameters](./api/reference.md#begintx) - Database connection settings

### 🧪 Testing

- [Running Tests](../README.md#testing) - How to run unit and integration tests
- [Performance Tests](../README.md#performance-tests) - Running performance benchmarks
- [Compatibility Tests](../README.md#integration-tests) - Testing PostgreSQL compatibility

### 📈 Performance

- [Benchmark Results](../README.md#-performance-benchmarks) - Current performance metrics
- [Optimization Techniques](./guides/embedded_usage.md#performance-considerations) - Performance tuning tips
- [Memory Management](./guides/embedded_usage.md#memory-management) - Understanding memory usage

## Getting Help

If you need help with PGLiteDB, you can:

1. **Check the FAQ** - Coming soon
2. **Browse Issues** - Check existing issues on GitHub
3. **Create an Issue** - Report bugs or request features
4. **Join the Community** - Coming soon

## Contributing

Contributions to PGLiteDB are welcome! Please see our [contribution guidelines](../README.md#contributing) for more information.

## License

PGLiteDB is released under the MIT License. See the [LICENSE](../LICENSE) file for details.

---

*Documentation last updated: November 2025*