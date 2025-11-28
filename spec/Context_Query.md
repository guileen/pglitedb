# Query Processing Context

★ Core Goal: Optimize query processing performance and accuracy through AST-based parsing

## Phase 8.7.11 Completed Status
✅ Query processing pipeline fully optimized with:
- Complete AST-based SQL parsing implementation
- Professional query optimizer with statistics collection framework
- ORDER BY/LIMIT/GROUP BY preservation in optimization
- Enhanced pg_query integration for complex SQL statements

## Key Architectural Improvements

## Phase 8.8 Planned Enhancements

### Query Optimizer Enhancement (8.8.4)
**Goal**: Implement cost-based optimization and advanced query rewriting
**Key Components**:
1. **Cost Model Implementation** - Based on collected statistics for accurate cost estimation
2. **JOIN Optimization** - Multiple algorithms (Nested Loop, Hash Join, Merge Join) with intelligent selection
3. **Advanced Rewrite Rules** - Predicate pushdown, constant folding, subquery unnesting
4. **Integration Points**:
   - `protocol/sql/cost_model.go` - Cost calculation framework
   - `protocol/sql/join_optimizer.go` - JOIN algorithm selection
   - `protocol/sql/optimizer.go` - Enhanced optimization rules

**Implementation Guide**: See `spec/GUIDE_QUERY_OPTIMIZER_ENHANCEMENT.md`

⚠️ **Note**: While query optimization is implemented, current critical infrastructure issues may affect query execution. See [GUIDE.md](./GUIDE.md) for priority fixes.

### AST-Based SQL Parsing Optimization

1. **Complete Regex Dependency Removal**
   - Removed all regexp imports from protocol/pgserver/server.go
   - Eliminated regex-based SQL parsing implementations
   - Replaced with professional AST-based parsing using pg_query_go
   - Weight: ★★★★★ (Eliminates security and correctness issues)

2. **RETURNING Clause Parsing**
   - Replaced regex-based RETURNING clause extraction with AST-based parsing
   - More accurate and secure parsing of RETURNING clauses
   - Support for complex RETURNING expressions
   - Weight: ★★★★☆ (Important for data modification operations)

3. **Parameter Binding Optimization**
   - Main parameter binding uses AST-based secure implementation
   - Regex implementation retained only as fallback (to be removed)
   - Type-safe parameter binding eliminates SQL injection risks
   - Weight: ★★★★★ (Critical security improvement)

### Query Planner Performance Improvements (protocol/sql/planner.go)

1. **Memory Allocation Optimization**
   - Pre-allocated slices with reasonable capacity to reduce allocations
   - Improved AST traversal efficiency
   - Optimized condition extraction algorithms
   - Weight: ★★★★☆ (Significant performance impact)

2. **Catalog Initialization Fix**
   - Fixed "catalog not initialized" error in PostgreSQL server
   - Ensured planner can correctly access catalog manager
   - Proper catalog passing from server to planner
   - Weight: ★★★★★ (Critical functionality fix)

### Query Optimizer Enhancements (protocol/sql/optimizer.go)

1. **Plan Preservation During Optimization**
   - All plan fields including OrderBy, GroupBy, Aggregates are properly copied
   - Weight: ★★★★★ (Critical for query correctness)
   - Common pitfall: Missing field copies in `applyRewriteRules` method

2. **Statistics Collection Integration**
   - Optimizer collects table statistics for better optimization decisions
   - Uses DataManager when available for real statistics
   - Weight: ★★★★☆ (Important for optimization quality)

3. **Query Rewrite Rules Framework**
   - Extensible framework for implementing query rewrite rules
   - Supports future optimization rules addition
   - Weight: ★★★★☆ (Important for extensibility)

### Performance Benchmarks

1. **Planner Condition Extraction**
   - Simple queries: 661.1 ns/op
   - Complex queries: 1,092 ns/op
   - Weight: ★★★★☆ (Demonstrates parsing efficiency)

## Component Interaction Documentation

### Data Flow Through Query Processing Pipeline

1. **Parsing Stage** (`protocol/sql/parser.go`)
   - Input: Raw SQL string
   - Output: AST representation (pg_query.Node or ParsedQuery)
   - Key files: `parser.go`, `pg_parser.go`

2. **Planning Stage** (`protocol/sql/planner.go`)
   - Input: Parsed AST
   - Process: Extract table, fields, conditions, ORDER BY, LIMIT, etc.
   - Output: Execution Plan
   - Key methods: `CreatePlan`, `extractSelectInfoFromPGNode`

3. **Optimization Stage** (`protocol/sql/optimizer.go`)
   - Input: Initial execution plan
   - Process: Apply rewrite rules, collect statistics
   - Output: Optimized execution plan
   - Key methods: `OptimizePlan`, `applyRewriteRules`

4. **Execution Stage** (`protocol/sql/executor.go`)
   - Input: Optimized execution plan
   - Process: Execute against storage engine using catalog
   - Output: ResultSet
   - Key methods: `Execute`, `executeSelect`
   - ⚠️ **Consolidated Implementation**: The executor has been enhanced with a unified interface that handles all statement types and includes improved error handling and logging

### Key Method Interactions

1. **Planner ↔ Optimizer**
   - `planner.CreatePlan()` calls `optimizer.OptimizePlan()`
   - Critical: All plan fields must be preserved during optimization

2. **Planner ↔ Catalog**
   - `NewPlannerWithCatalog()` initializes planner with catalog
   - `SetCatalog()` updates catalog for existing planner

3. **Optimizer ↔ Catalog**
   - `NewQueryOptimizerWithDataManager()` provides statistics collection
   - Statistics used for optimization decisions

## Troubleshooting Guide

### Common Issues and Solutions

1. **ORDER BY Information Lost**
   - **Symptom**: Tests fail with `assert.Len(t, plan.OrderBy, 1)` expecting 1 but getting 0
   - **Cause**: Missing OrderBy copy in `applyRewriteRules` method
   - **Solution**: Verify `optimizedPlan.OrderBy = make([]OrderBy, len(plan.OrderBy))` and `copy(optimizedPlan.OrderBy, plan.OrderBy)` exist

2. **Catalog Not Initialized**
   - **Symptom**: "catalog not initialized" errors
   - **Cause**: Planner created without catalog or catalog not set
   - **Solution**: Use `NewPlannerWithCatalog()` or call `SetCatalog()` before execution

3. **Parse Failures**
   - **Symptom**: Parsing errors for valid SQL
   - **Cause**: Using wrong parser or parser not properly configured
   - **Solution**: Ensure using `PGParser` for complex SQL, check parser initialization

### Debugging Approaches

1. **For Failing Tests**
   - Add debug logging in `CreatePlan` method to see plan before/after optimization
   - Check that `extractSelectInfoFromPGNode` correctly extracts ORDER BY
   - Verify `applyRewriteRules` preserves all plan fields
   - **Specific to ORDER BY issues**: Ensure `optimizedPlan.OrderBy` is properly initialized and copied in `applyRewriteRules`

2. **For Performance Issues**
   - Profile `extractSelectInfoFromPGNode` for bottlenecks
   - Check memory allocations in plan creation
   - Monitor statistics collection overhead

### Common Debugging Scenario: ORDER BY Information Lost

This is a frequent issue where tests fail because ORDER BY information is not preserved through optimization:

1. **Symptoms**:
   - Tests expecting `plan.OrderBy` to have elements but getting empty slice
   - Integration tests failing with `assert.Len(t, plan.OrderBy, 1)` assertions

2. **Root Cause**:
   - Missing or incomplete copy of `OrderBy` field in `applyRewriteRules` method

3. **Debugging Steps**:
   - Add logging in `planner.CreatePlan` before and after `p.optimizer.OptimizePlan` call
   - Check that `extractSelectInfoFromPGNode` correctly populates `plan.OrderBy`
   - Verify `applyRewriteRules` in `optimizer.go` properly initializes and copies `OrderBy` slice:
     ```go
     optimizedPlan := &Plan{
         // ... other fields
         OrderBy: make([]OrderBy, len(plan.OrderBy)),
         // ... other fields
     }
     copy(optimizedPlan.OrderBy, plan.OrderBy)
     ```

4. **Prevention**:
   - Always ensure all slice fields in Plan struct are properly copied in optimization
   - Add test cases that specifically verify plan field preservation
   - Use debug logging during development to trace plan transformations

## Important Context About Changes

### Why These Optimizations Were Necessary

1. **Security Concerns**
   - Regex-based parsing was vulnerable to SQL injection attacks
   - String replacement without proper validation posed security risks
   - Lack of type safety in parameter binding
   - Weight: ★★★★★ (Critical security requirement)

2. **Correctness Issues**
   - Regex expressions could not handle all SQL syntax correctly
   - Complex queries often failed to parse properly
   - Edge cases in SQL syntax caused parsing errors
   - Weight: ★★★★★ (Fundamental correctness requirement)

3. **Maintainability Problems**
   - Complex regex patterns were difficult to maintain and extend
   - Debugging regex-based parsing was challenging
   - Adding new SQL features required complex regex modifications
   - Weight: ★★★★☆ (Important development efficiency concern)

### How They Improve the System

1. **Enhanced Security**
   - AST-based parsing eliminates SQL injection vulnerabilities
   - Type-safe parameter binding ensures data integrity
   - Professional parsing library provides robust security
   - Weight: ★★★★★ (Critical improvement)

2. **Improved Correctness**
   - AST-based parsing handles complex SQL syntax correctly
   - Professional parser supports full PostgreSQL compatibility
   - Better error reporting and handling
   - Weight: ★★★★★ (Fundamental quality improvement)

3. **Better Performance**
   - Optimized AST traversal algorithms
   - Reduced memory allocations in query processing
   - Efficient catalog access for metadata operations
   - Weight: ★★★★☆ (Noticeable performance gains)

### Problems Solved

1. **SQL Injection Vulnerabilities**
   - Regex-based parsing was inherently insecure
   - Parameter binding lacked proper validation
   - String replacement techniques were vulnerable to injection
   - Weight: ★★★★★ (Critical security issue resolved)

2. **Parsing Accuracy Issues**
   - Regex could not correctly parse complex SQL statements
   - Edge cases in SQL syntax caused failures
   - Limited PostgreSQL compatibility
   - Weight: ★★★★★ (Fundamental correctness issue resolved)

3. **Catalog Access Problems**
   - "catalog not initialized" errors prevented query execution
   - Planner could not access metadata for query planning
   - Improper catalog initialization in server startup
   - Weight: ★★★★★ (Critical functionality restored)

## Implementation Patterns and Best Practices

### Plan Field Preservation
When copying plans in optimization, always ensure all fields are properly copied:
```go
optimizedPlan := &Plan{
    // ... initialize all fields
    OrderBy: make([]OrderBy, len(plan.OrderBy)),
    // ... other slice fields
}
copy(optimizedPlan.OrderBy, plan.OrderBy)
```

### Error Handling
Always preserve original plan when optimization fails:
```go
optimizedPlan, err := p.optimizer.OptimizePlan(plan)
if err != nil {
    return plan, nil // Return original plan
}
```

### Query Optimization Framework
Follow the optimizer framework patterns for extensibility:
```go
// In applyRewriteRules method
optimizedPlan := &Plan{
    // Copy all fields from original plan
    OrderBy: make([]OrderBy, len(plan.OrderBy)),
    GroupBy: make([]GroupBy, len(plan.GroupBy)),
    Aggregates: make([]Aggregate, len(plan.Aggregates)),
    // ... other fields
}
copy(optimizedPlan.OrderBy, plan.OrderBy)
copy(optimizedPlan.GroupBy, plan.GroupBy)
copy(optimizedPlan.Aggregates, plan.Aggregates)
```

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