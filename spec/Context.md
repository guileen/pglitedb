# Project Context Navigation

★ Core Goal: Maintain efficient context management for AI agents working with PGLiteDB, focusing on maintainability and technical debt reduction

This file serves as the central index for project context. All AI agents should reference this file first, then proceed to the relevant aspect-specific context files.

## Current Focus: Quality Assurance & Stability

✅ **Latest Achievement**: Achieved 100% PostgreSQL regress test pass rate (228/228 tests) with strong performance benchmarks (2,568 TPS) and 3.894ms average latency. Successfully completed all Phase 1-3 architectural improvements including comprehensive resource leak detection, dynamic pool sizing, system catalog caching, and query result streaming. Established detailed performance optimization roadmap in [PERFORMANCE_OPTIMIZATION_PLAN.md](./PERFORMANCE_OPTIMIZATION_PLAN.md) targeting 6,500 TPS. Phase 4 quality assurance work is now well underway with comprehensive testing infrastructure in place.
✅ **Phase 1 Status**: Fully completed with all engine decomposition initiatives successfully implemented
✅ **Phase 2 Status**: Fully completed with interface refinement and protocol layer enhancements
✅ **Phase 3 Status**: Fully completed with performance optimization and resource management enhancements
✅ **Phase 4 Status**: In progress with comprehensive quality assurance and stability improvements
🎯 **Immediate Focus**: Phase 4 quality assurance and stability improvements with emphasis on extended testing and production readiness validation
- [x] 当前重点：提升regress测试至100%，每个版本每个版本都要有性能测试报告，对比上一个版本的性能提升。
- [x] refactor scripts/run_regress.sh 文件名改为regress_yymmdd_hhmmss.out ,直接展示测试的output，控制台也显示。
- [x] 性能测试要有对比性，和官方postgresql的对比，和自己历史版本的对比。测试报告可能要brief到某个目录。j还有测试覆盖率等等，这些对于社区的运营也非常重要。当regress全部通过后，提升版本到 v0.2.
- [x] README.md 的更新，更加具有传播性。核心卖点：1. 高性能 2. 嵌入式 3. 全AI自动化编写优化。
- [x] Comprehensive test coverage implementation completed for all core components
- [x] Property-based testing framework implemented and validated
- [ ] fix bug: 即使服务启动失败（比如端口被占用），make regress_bench 也通过了。其实测试的是别的端口服务。
- [ ] README.md 要强调 100% PostgreSQL regress test pass， 最好直接贴出命令执行过程和结果。再提供一些psql直接连接的例子（要自己真实测试过，快速启动服务并且能直接用psql连接上）。
- [ ] README 中go get 之后再 go run 这个例子不好。不优雅。
- [ ] embedded client 导入路径不优雅。
- [ ] Tenant isolation, multi-tenancy support. 是否有必要？多租户情况下如何保证和postgresql的兼容？是否服务启动或连接或认证环节需要更多的认证信息？
- [ ] 嵌入式使用时的文档和示例代码，是否更方便？比sqlite是否更方便？有没有竞争优势？
- [ ] 高可用方面的考虑，Cluster 模式将会是一个巨大的调整，整个架构都将变得更复杂。但若是要支持的话，不得不更早地思考这个问题。

## Context Quality Feedback Loop
⚠️ **Continuous Improvement**: This context system incorporates user reflections to continuously improve quality and relevance. See [REFLECT.md](./REFLECT.md) for contribution guidelines.

## Key Implementation Insights from Reflection
✅ **Recent Implementation Successes**:
- Interface-driven development enabled clean separation between transaction logic and storage implementation
- Bulk operation efficiency significantly improved through storage-level batching
- Proper index management established complex but necessary relationships between catalog metadata and physical storage
- Error handling and resource cleanup properly implemented in transaction rollback functionality
- Successful engine modularization with significant file size reductions and improved maintainability (see [REFLECT_ENGINE_MODULARIZATION.md](./REFLECT_ENGINE_MODULARIZATION.md) for detailed outcomes)
- **Aggregate Function Implementation**: Added support for COUNT and other aggregate functions with proper AST traversal and plan structure population
- **System Table Implementation**: Added pg_database system table provider for database metadata queries
- **Interface Refinement**: Completed segregation of StorageEngine interface into specialized interfaces for better modularity and testability
- **Protocol Layer Enhancements**: Updated catalog components to use specialized interfaces while maintaining backward compatibility
- **Performance Foundation Improvements**: Implemented connection pooling, query execution pipeline, and memory management tuning for significant performance gains
- **Storage Engine Performance Optimizations**: Implemented object pooling and batch operations reducing memory allocations by up to 90% in key operations
- **System Table Query Fixes**: Resolved issues with system table recognition and query execution paths for full PostgreSQL compatibility
- **Full Transaction Management & MVCC**: Implemented ACID-compliant transaction system with complete MVCC support and all isolation levels
- **Statistics Collection Framework**: Implemented professional statistics collection with table and column statistics for cost-based optimization
- **Comprehensive Resource Leak Detection**: Implemented complete leak detection system for iterators, transactions, connections, file descriptors, and goroutines with stack trace capture and automated monitoring

## Architectural Improvement Roadmap Status
✅ **Progress Tracking**: Following the phased implementation plan from [GUIDE.md](./GUIDE.md) and [ARCHITECT-REVIEW.md](./ARCHITECT-REVIEW.md)

### Phase 1: Foundation (Weeks 1-2) - ✅ COMPLETED
- ✅ Engine file decomposition initiatives completed successfully
  - Reduced `engine/pebble/engine.go` from over 10KB to < 200 lines
  - Split `engine/pebble/transaction_manager.go` from 14.6KB to smaller, focused files
  - Eliminated the monolithic `engine/pebble/query_processor.go` by distributing its functionality
- ✅ Index operations extracted to dedicated packages (`engine/pebble/indexes/`)
- ✅ Filter evaluation logic moved to specialized modules
- ✅ ID generation functionality separated (`idgen/` package)
- ✅ Transaction implementations split into regular and snapshot variants

### Phase 2: Interface Refinement (Weeks 3-4) - ✅ COMPLETED
- ✅ Complete segregation of `StorageEngine` interface
- ✅ Define all specialized interfaces in `engine/types/`
- ✅ Update dependent code to use specific interfaces
- ✅ Complete protocol layer enhancements

### Phase 3: Performance Optimization (Weeks 5-8) - ✅ COMPLETED
- ✅ Connection pooling with health checking and advanced lifecycle management
- ✅ Query execution pipeline with batch processing and worker pools
- ✅ Memory management with object pooling for reduced allocations
- ✅ Storage engine performance optimizations with object pooling and batch operations
- ✅ Comprehensive resource leak detection implementation
- ✅ Dynamic pool sizing capabilities
- ✅ System catalog caching with LRU eviction
- ✅ Concurrency and thread safety improvements
- ✅ Query result streaming for large result sets
- ✅ Advanced caching strategies
- ✅ Performance monitoring and metrics collection

### Phase 4: Quality Assurance (Weeks 9-10) - In Progress
- ✅ Comprehensive test coverage plan implemented with detailed test cases for all components
- ✅ Property-based testing for filter evaluation and complex logic validation
- ✅ Comprehensive concurrency testing implementation
- ✅ Expansion of edge case testing coverage
- ✅ Load testing infrastructure development
- ✅ Automated performance regression testing

## Critical Infrastructure Fix Priority
✅ **Completed**: Critical infrastructure stabilization successfully completed
- System table implementation fully functional with proper OID consistency and referential integrity
- DDL operations properly persisting metadata in system tables with consistent OID generation
- Query execution paths restored with system catalog lookup functionality
- Complex system table relationships properly maintained (pg_class.oid ↔ pg_attribute.attrelid, etc.)
- System table query fixes resolved issues with recognition and execution paths for full PostgreSQL compatibility

## Phase 8.8 Completed Status
✅ All Phase 8.8 tasks completed successfully including:
- Statistics Collection Framework implementation with table and column statistics
- DDL Parser Enhancement with CREATE INDEX, DROP INDEX, and advanced ALTER TABLE support
- System Tables Extension with pg_stat_* series, pg_index, and pg_inherits implementation
- Query Optimizer Enhancement with cost-based optimization, JOIN optimization, and advanced rewrite rules

## Phase 9.1 Completed Status
✅ All Phase 9.1 tasks completed successfully including:
- Full ACID-compliant transaction management with BEGIN/COMMIT/ROLLBACK/SAVEPOINT
- Complete Multi-Version Concurrency Control (MVCC) for read consistency
- Full isolation level support (READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SNAPSHOT ISOLATION, SERIALIZABLE)
- Advanced deadlock detection and prevention mechanisms
- Comprehensive savepoint support for nested transactions
- Transaction logging and recovery mechanisms with WAL (Write-Ahead Logging)

## Key Files Navigation
- Database Operations: `spec/Context_Database.md`
- Query Processing: `spec/Context_Query.md`
- Transaction Management: `spec/Context_Transaction.md`
- System Catalog: `spec/Context_Catalog.md` ⚠️ **Key Focus**: OID consistency and system table relationships
- Storage Engine: `spec/Context_Engine.md` ⚠️ **Key Focus**: Interface-driven design and bulk operation efficiency
- DDL Parser: `spec/Context_DDL.md` ⚠️ **Key Focus**: Metadata persistence with consistent OID generation
- Technical Debt Reduction: `spec/Context_TechDebt.md` ⚠️ **Key Focus**: Maintainability improvements and refactoring efforts
- Resource Management & Leak Detection: `spec/Context_ResourceManagement.md` ⚠️ **Key Focus**: Object pooling and leak detection mechanisms
- Component Cross-Reference: `spec/Context_CrossReference.md`
- Troubleshooting Guide: `spec/Context_Troubleshooting.md`
- Logging System: `spec/Context_Logging.md` ⚠️ **Key Focus**: Structured logging with slog-based implementation

## Documentation Navigation
- Master Documentation Navigation: `spec/DOCUMENTATION_NAVIGATION.md` ⚠️ **Key Focus**: Organized access to all project documentation

## Implementation Roadmap Guides
- Master Architecture Improvement Roadmap: `spec/GUIDE.md` ⚠️ **UPDATED LONG-TERM ROADMAP**
- Architectural Review Findings: `spec/ARCHITECT-REVIEW.md` ⚠️ **UPDATED WITH CURRENT STATUS**
- Transaction Management & MVCC: `spec/GUIDE_TRANSACTION_MVCC.md`
- Security Features: `spec/GUIDE_SECURITY.md`
- Advanced PostgreSQL Compatibility: `spec/GUIDE_POSTGRESQL_COMPATIBILITY.md`
- Performance & Scalability: `spec/GUIDE_PERFORMANCE_SCALABILITY.md`
- Reliability & Operations: `spec/GUIDE_RELIABILITY_OPERATIONS.md`
- Phases Completed Summary: `spec/PHASES_COMPLETED_SUMMARY.md`
- Performance Optimization Plan: `spec/PERFORMANCE_OPTIMIZATION_PLAN.md` ⚠️ **NEW PERFORMANCE ROADMAP** ⚠️ **NEW SUMMARY DOCUMENT**

## Technical Debt Reduction Plans
- Maintainability & Technical Debt Reduction Plan: `spec/MAINTAINABILITY_TECHNICAL_DEBT_REDUCTION_PLAN.md`
- Technical Debt Reduction Implementation Plan: `spec/TECHNICAL_DEBT_REDUCTION_IMPLEMENTATION_PLAN.md`
- Immediate Actions Implementation Plan: `spec/IMMEDIATE_ACTIONS_IMPLEMENTATION_PLAN.md`
- Resource Management Enhancement Plan: `spec/RESOURCE_MANAGEMENT_ENHANCEMENT_PLAN.md`
- Comprehensive Improvement Plan Summary: `spec/COMPREHENSIVE_IMPROVEMENT_PLAN_SUMMARY.md`

## Component Interaction Diagram
```mermaid
graph LR
    A[Client] --> B[PG Server]
    B --> C[SQL Parser]
    C --> D[Query Planner]
    D --> E[Query Optimizer]
    D --> F[Executor]
    E --> F
    F --> G[Catalog Manager]
    F --> H[Storage Engine]
    G --> H
    C --> I[DDL Parser]
    I --> G
```

## Troubleshooting Quick Reference
⚠️ Common Issues and Solutions:
1. **ORDER BY Not Preserved**: Check `protocol/sql/optimizer.go` - ensure `applyRewriteRules` properly copies all plan fields including OrderBy
2. **Catalog Not Initialized**: Verify `NewPlannerWithCatalog` and `SetCatalog` methods in `protocol/sql/planner.go`
3. **Parse Failures**: Check if using correct parser (`PGParser` vs custom parser) in `protocol/sql/parser.go`

## Development Workflow Guidance
1. **Bug Fixing Path**:
   - Start with failing test in `protocol/sql/integration_test.go`
   - Trace through `planner.go` → `optimizer.go` → `executor.go`
   - Check plan preservation in optimization steps

2. **Feature Implementation Path**:
   - Define schema in `catalog/` 
   - Implement parsing in `parser.go`
   - Add planning logic in `planner.go`
   - Optimize in `optimizer.go`
   - Execute in `executor.go`

## Recent Key Improvements (Phase 8.8 Completed)

### Statistics Collection Framework
- ✅ Professional statistics collection implementation with table and column statistics
- ✅ Integration with query optimizer for cost-based optimization
- ✅ ANALYZE command support for manual statistics collection
- ✅ Automatic statistics collection mechanisms

### DDL Parser Enhancement
- ✅ CREATE INDEX and DROP INDEX support with multiple index types
- ✅ Enhanced ALTER TABLE with ADD/DROP CONSTRAINT operations
- ✅ Constraint validation framework implementation
- ✅ Integration with system tables (pg_indexes, pg_constraint)

### System Tables Extension
- ✅ pg_stat_* series implementation for statistics querying
- ✅ pg_index system table for index metadata
- ✅ pg_inherits system table for table inheritance relationships
- ✅ pg_database system table for database metadata queries
- ✅ Full integration with catalog manager

### Query Optimizer Enhancement
- ✅ Cost-based optimization with statistics-driven decisions
- ✅ JOIN optimization with multiple algorithm support
- ✅ Advanced query rewrite rules implementation
- ✅ Query plan caching for repeated queries

### Engine Architecture Improvements
- ✅ Interface-driven storage engine design enabling clean separation of concerns
- ✅ Bulk operation efficiency through storage-level batching capabilities
- ✅ Transaction pattern consistency with unified APIs for regular and snapshot transactions
- ✅ Improved resource management with proper error handling and cleanup

## Phase 9.1 Implementation (Completed)

### Full Transaction Management & MVCC
- ✅ ACID-compliant transaction implementation with complete state management
- ✅ Multi-Version Concurrency Control for read consistency and snapshot isolation
- ✅ Support for all PostgreSQL isolation levels
- ✅ Savepoint management for nested transactions
- ✅ Deadlock detection and prevention mechanisms
- ✅ Transaction logging with Write-Ahead Logging for durability and recovery

For detailed technical implementation, see [Transaction Management & MVCC Guide](./GUIDE_TRANSACTION_MVCC.md)

## Current Focus: Quality Assurance & Stability Implementation
🎯 **Priority Areas for Phase 4**:

### 1. Comprehensive Testing Implementation
- [x] Implement comprehensive concurrency testing
- [x] Add race condition detection tests
- [x] Create deadlock scenario tests
- [x] Add performance benchmarking under load
- [x] Expand edge case testing coverage
- [x] Implement error recovery scenario tests
- [x] Create timeout and cancellation tests
- [x] Add malformed input handling tests

### 2. Automated Quality Assurance
- [x] Implement automated performance regression testing
- [x] Add continuous coverage monitoring
- [x] Implement property-based testing for complex logic validation
- [ ] Target: Achieve 90%+ code coverage for core packages

### 3. Production Stability Validation
- [ ] Run extended stress testing (72-hour duration)
- [ ] Validate production readiness
- [ ] Document stability and reliability metrics

## Implementation Quality Improvements
✅ **Key Quality Enhancements**:
- **Interface-Driven Development**: Well-defined interfaces enable clean separation between components
- **Modular Architecture**: Breaking down large files into smaller, focused modules improves maintainability
- **Consistent Error Handling**: Proper resource cleanup in error paths ensures system stability
- **Comprehensive Testing**: Enhanced test coverage for error conditions and edge cases
- **Aggregate Function Support**: Added basic support for COUNT and other aggregate functions with GROUP BY clause parsing
- **Performance Foundation**: Connection pooling, query pipeline, and memory management improvements provide significant performance gains
- **Full Transaction Support**: ACID-compliant transactions with MVCC and all isolation levels
- **Statistics Collection**: Professional statistics framework for cost-based query optimization

## Access Requirements
❗ All context users must provide:
1. Reflections on their task outcomes
2. Ratings of context usefulness (1-10 scale)
3. Specific feedback on referenced sections

This feedback is essential for continuous context improvement and must be submitted with every context access.

See [REFLECT.md](./REFLECT.md) for detailed reflection guidelines and examples.

## Maintenance Guidelines
⚠️ Context files are limited to 5000 words
⚠️ Use weight markers for prioritization
⚠️ Follow the two-file lookup rule strictly