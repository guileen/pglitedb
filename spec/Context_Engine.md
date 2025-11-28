# Storage Engine Context

★ Core Goal: Document storage engine implementation and management for PGLiteDB with focus on maintainability and technical debt reduction based on architectural review findings

This file provides context about the completed storage engine implementation, including transaction management, bulk operations, and index management, with emphasis on successful refactoring efforts aligned with the architecture improvement roadmap.

## Implementation Status: COMPLETED ✅

All Phase 1-3 engine improvements have been successfully completed.

## Implementation Insights from Reflection
✅ **Key Learnings**:
- Interface-driven development enabled clean separation between transaction logic and storage implementation
- Bulk operation efficiency significantly improved through storage-level batching capabilities
- Transaction pattern consistency achieved with unified APIs for regular and snapshot transactions
- Error handling importance highlighted through proper resource cleanup in error paths
- Engine modularization successfully achieved significant maintainability improvements with file size reductions
- Package organization significantly improves code navigation and understanding
- Interface Segregation Principle leads to more flexible and maintainable code
- Successful implementation of comprehensive storage engine with full transaction support

## Core Engine Components (Post-Phase 1 Refactoring)

### 1. Pebble Storage Engine (`engine/pebble/engine.go`) - < 200 lines ✅ COMPLETED
- Coordinates transaction-based bulk operations
- Manages ID generation for tables and indexes
- Provides entry point for all storage engine operations
- ✅ **Achieved**: Reduced from over 10KB to < 200 lines containing focused core interface implementation
- Weight: ★★★★★ (Core storage functionality)

### 2. Transaction Manager (`engine/pebble/transaction_manager.go`) - Reduced to focused files ✅ COMPLETED
- Implements core transaction logic for both regular and snapshot transactions
- Added complete UpdateRows/DeleteRows functionality for bulk operations
- Provides unified interface for transaction management
- ✅ **Achieved**: Split into `transaction_regular.go` and `transaction_snapshot.go` with focused responsibilities
- Weight: ★★★★★ (Transaction processing foundation)

### 3. Query Processor (`engine/pebble/query_processor.go`) - ELIMINATED ✅ COMPLETED
- Handles query execution coordination
- Manages query planning and optimization
- Coordinates with catalog and storage systems
- ✅ **Achieved**: Eliminated by distributing functionality to specialized packages
- Weight: ★★★★☆ (Query processing)

### 4. Specialized Packages Created in Phase 1
- `idgen/` package for ID generation functionality
- `engine/pebble/indexes/` package for all index-related operations
- `engine/pebble/operations/query/` package with separate files for insert, update, delete operations
- `engine/pebble/utils/` package for utility functions like range building

### 4. Engine Interfaces (`engine/interfaces.go`)
- Defines clear contract for all storage engine operations
- Establishes consistent API for bulk operations and index management
- Enables extensibility through interface design
- ❗ **Target**: Segregate into focused interfaces following Interface Segregation Principle
- Weight: ★★★★★ (Interface foundation)

### 5. Engine Types (`engine/types/types.go`)
- Defines data structures for bulk operations
- Establishes consistent type definitions across components
- Supports index and row operation specifications
- Weight: ★★★☆☆ (Type definitions)

## Key Implementation Patterns

### Interface-Driven Development
Using well-defined interfaces enables clean separation between transaction logic and storage implementation, making the codebase more maintainable and testable.

### Bulk Operation Efficiency
The initial naive implementation of UpdateRows/DeleteRows using individual row operations highlighted the performance implications of not leveraging storage-level batching capabilities.

### Transaction Pattern Consistency
Implementing both regular and snapshot transactions with consistent APIs revealed the importance of avoiding code duplication through proper inheritance or composition patterns.

## Completed Refactoring Work (Phase 1)

### Priority 1: Engine Decomposition (Weeks 1-2) - ✅ COMPLETED
1. **Extract Indexing Operations** - ✅ COMPLETED
   - Moved to `engine/pebble/indexes/` package
   - Created dedicated index management functionality
   - Weight: ★★★★★ (Critical for modularity)

2. **Move Filter Evaluation Logic** - ✅ COMPLETED
   - Extracted to specialized modules
   - Separated filter evaluation and expression handling
   - Weight: ★★★★★ (Critical for separation of concerns)

3. **Separate ID Generation** - ✅ COMPLETED
   - Created `idgen/` package for ID generation functionality
   - Isolated ID generation logic from core engine
   - Weight: ★★★★★ (Critical for focused responsibilities)

4. **Reduce Core Engine Size** - ✅ COMPLETED
   - Achieved: Reduced `engine.go` from over 10KB to < 200 lines
   - Focus only on core interface implementation
   - Weight: ★★★★★ (Critical for maintainability)

### Current Focus: Interface Segregation (Continuing from Phase 2)
- Complete segregation of `StorageEngine` interface into specialized interfaces
- Define all specialized interfaces in `engine/types/`
- Update dependent code to use specific interfaces

### Priority 2: Interface Segregation (Weeks 3-4)
1. **Segregate StorageEngine Interface**
   - Split 37+ method interface into focused interfaces
   - Create `RowOperations`, `IndexOperations`, `TransactionOperations`, `IDGeneration`
   - Weight: ★★★★★ (Critical for testability)

2. **Update Dependent Code**
   - Modify consumers to accept specific interfaces
   - Maintain backward compatibility through adapter patterns
   - Weight: ★★★★☆ (Important for adoption)

## Performance Considerations

### Bulk Operations
- Storage-level batching significantly improves performance over individual row operations
- Efficient resource utilization through proper batch processing
- Reduced I/O overhead through batched writes

### Resource Management
- Proper error handling ensures resource cleanup in all paths
- Connection pooling and lifecycle management for optimal resource utilization
- Memory optimization techniques for large dataset processing
- ✅ Comprehensive resource leak detection for iterators, transactions, connections, file descriptors, and goroutines
- Object pooling with leak tracking and metrics collection

## Troubleshooting Guide

### Common Issues and Solutions

1. **Bulk Operation Performance Issues**
   - **Symptom**: Slow bulk operations
   - **Cause**: Individual row operations instead of batched processing
   - **Solution**: Use storage-level batching capabilities

2. **Transaction Management Issues**
   - **Symptom**: Inconsistent transaction states or resource leaks
   - **Cause**: Improper error handling or resource cleanup
   - **Solution**: Ensure proper resource cleanup in all error paths

3. **Index Management Problems**
   - **Symptom**: Index creation/deletion failures
   - **Cause**: Metadata inconsistency or storage operation failures
   - **Solution**: Verify metadata management and storage integration

## Implementation Quality Improvements
✅ **Key Quality Enhancements**:
- **Interface-Driven Design**: Well-defined interfaces enable clean separation of concerns
- **Modular Architecture**: Breaking down large files into smaller, focused modules improves maintainability
- **Comprehensive Testing**: Enhanced test coverage for error conditions and edge cases
- **Performance Optimization**: Storage-level batching for bulk operations
- **Successful Engine Modularization**: Phase 1 successfully reduced monolithic components and created focused packages
- **Improved Code Organization**: Thoughtful package organization significantly improves code navigation and understanding
- **Resource Leak Detection**: Comprehensive leak detection system for tracking iterators, transactions, connections, file descriptors, and goroutines
- **Dynamic Pool Sizing**: Adaptive pool size adjustment based on usage patterns for optimal resource utilization

## Current Architecture (Post-Phase 1 Refactoring)
```
engine/pebble/
├── engine.go                    # Core engine interface and factory (< 200 lines) ✅
├── transaction_regular.go       # Regular transaction implementation ✅
├── transaction_snapshot.go      # Snapshot transaction implementation ✅
├── resource_manager.go          # Resource lifecycle management
├── id_generator.go              # ID generation logic (moved to idgen/ package) ✅
├── indexes/                     # Index operations and management package ✅
│   ├── manager.go
│   └── utils.go
├── operations/                  # Organized by operation type ✅
│   ├── query/                   # Query operations package
│   │   ├── insert.go
│   │   ├── update.go
│   │   ├── delete.go
│   │   └── select.go
│   ├── scan/
│   ├── modify/
│   ├── batch/
│   └── transaction/
├── utils/                       # Utility functions package ✅
└── handlers/
```

## Target Architecture (Phase 2+ Goals)
```
engine/pebble/
├── engine.go                    # Core engine interface and factory (< 100 lines)
├── transaction_manager.go       # Transaction coordination (< 300 lines)
├── query_processor.go           # Query execution coordination (< 300 lines)
├── resource_manager.go          # Resource lifecycle management (< 200 lines)
├── id_generator.go              # ID generation logic (< 150 lines)
├── filter_evaluator.go          # Filter evaluation and expression handling (< 200 lines)
├── index_manager.go             # Index operations and management (< 300 lines)
├── scanner.go                   # Scan operations coordination (< 200 lines)
├── operations/                  # Organized by operation type
│   ├── scan/
│   ├── modify/
│   ├── batch/
│   └── transaction/
├── utils/
└── handlers/
```

## Related Documentation
- Master Architecture Improvement Roadmap: `spec/GUIDE.md`
- Architectural Review Findings: `spec/ARCHITECT-REVIEW.md`
- Storage Engine Refactoring Plan: `spec/TECHNICAL_DEBT_REDUCTION_IMPLEMENTATION_PLAN.md`
- Implementation Roadmap: `spec/IMMEDIATE_ACTIONS_IMPLEMENTATION_PLAN.md`

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