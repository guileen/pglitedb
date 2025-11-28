# Troubleshooting Guide for Common Development Issues

★ Core Goal: Provide quick solutions for common problems encountered during development

## Phase 8.7.11 Completed Status
✅ All core troubleshooting guides updated with:
- ORDER BY preservation fixes
- Catalog initialization resolutions
- System table query solutions
- Parse failure debugging approaches

## Debugging Query Processing Issues

### ORDER BY Information Lost in Optimization

**Problem**: Tests fail with `assert.Len(t, plan.OrderBy, 1)` expecting 1 but getting 0.

**Root Cause**: The optimizer's `applyRewriteRules` method is not properly copying all plan fields.

**Solution Steps**:
1. Check `protocol/sql/optimizer.go` in the `applyRewriteRules` function
2. Ensure `optimizedPlan` struct initialization includes:
   ```go
   OrderBy: make([]OrderBy, len(plan.OrderBy)),
   ```
3. Verify the copy operation:
   ```go
   copy(optimizedPlan.OrderBy, plan.OrderBy)
   ```
4. Add debug logging to trace plan contents:
   ```go
   // In planner.go CreatePlan method, before and after optimization
   log.Printf("Before optimization: OrderBy=%v", plan.OrderBy)
   log.Printf("After optimization: OrderBy=%v", optimizedPlan.OrderBy)
   ```

### Catalog Not Initialized Errors

**Problem**: "catalog not initialized" errors during query execution.

**Root Cause**: Planner created without catalog or catalog not properly set.

**Solution Steps**:
1. Ensure using `NewPlannerWithCatalog` instead of `NewPlanner`:
   ```go
   planner := NewPlannerWithCatalog(parser, catalogMgr)
   ```
2. Or call `SetCatalog` before execution:
   ```go
   planner.SetCatalog(catalogMgr)
   ```
3. Verify catalog manager is properly initialized in server startup
4. Check that executor is created with catalog in `NewPlannerWithCatalog`

### System Table Query Failures

**Problem**: Queries against pg_indexes or pg_constraint fail or return no data.

**Root Cause**: System tables not properly registered or populated.

**Solution Steps**:
1. Check `catalog/system_tables.go` for proper system table registration
2. Verify `PopulateSystemTables` method correctly implements data population
3. Ensure system tables are registered during catalog initialization
4. Add debug logging to trace system table query execution

### DDL Parse Failures

**Problem**: DDL statements fail to parse or execute correctly.

**Root Cause**: Unsupported DDL syntax or missing parser rules.

**Solution Steps**:
1. Check `protocol/sql/ddl_parser.go` for proper AST node handling
2. Verify DDL statement syntax matches supported grammar
3. Ensure catalog manager properly handles schema changes
4. Add debug logging to trace DDL parsing and execution flow

**Phase 8.8 Enhancement**: Additional DDL operations will require extended parser rules and AST nodes.

## Component Interaction Debugging

### Tracing Data Flow Through Components

**When to Use**: Understanding how data flows from parsing to execution.

**Steps**:
1. Start with `protocol/sql/integration_test.go` failing test case
2. Follow execution through:
   - `planner.CreatePlan()` in `protocol/sql/planner.go`
   - `optimizer.OptimizePlan()` in `protocol/sql/optimizer.go`
   - `executor.Execute()` in `protocol/sql/executor.go`
3. Add logging at each step to trace data transformation

### Verifying Component Integration

**When to Use**: When components don't seem to work together correctly.

**Steps**:
1. Check that interfaces are properly implemented
2. Verify constructor methods pass dependencies correctly
3. Ensure method signatures match interface definitions
4. Add integration tests for component boundaries

## Performance Issue Debugging

### Identifying Bottlenecks

**When to Use**: When query processing is slower than expected.

**Steps**:
1. Profile with Go's built-in profiler:
   ```bash
   go test -bench=. -cpuprofile=cpu.prof
   go tool pprof cpu.prof
   ```
2. Check memory allocations:
   ```bash
   go test -bench=. -memprofile=mem.prof
   ```
3. Focus on hot paths:
   - `planner.extractSelectInfoFromPGNode`
   - `optimizer.applyRewriteRules`
   - `engine.GetRowBatch`

### Memory Usage Issues

**When to Use**: High memory consumption or frequent garbage collection.

**Steps**:
1. Check slice pre-allocation in planner and engine
2. Verify buffer reuse in batch operations
3. Profile memory allocations:
   ```bash
   go test -bench=. -benchmem
   ```

## Test Failure Debugging

### Failing Integration Tests

**When to Use**: Integration tests fail unexpectedly.

**Steps**:
1. Run specific failing test:
   ```bash
   go test -v -run TestName
   ```
2. Add detailed logging in the test and related components
3. Check that test setup matches expected state
4. Verify test assertions match actual implementation

### Test Setup Issues

**When to Use**: Tests fail due to improper setup.

**Steps**:
1. Check catalog initialization in test setup
2. Verify test data is properly inserted
3. Ensure planner and executor are properly configured
4. Confirm parser is correctly initialized

## Common Pitfalls and How to Avoid Them

### 1. Missing Field Copies in Optimization
**Pitfall**: Forgetting to copy all fields when creating optimized plans.
**Prevention**: Use comprehensive struct initialization and copy operations.

### 2. Improper Error Handling
**Pitfall**: Not handling errors at the right level or providing poor error messages.
**Prevention**: Always wrap errors with context and handle at appropriate levels.

### 3. Race Conditions in Concurrent Access
**Pitfall**: Concurrent access to shared resources without proper synchronization.
**Prevention**: Use mutexes for shared state and verify thread safety.

## Quick Reference for Key Files

### Query Processing
- `protocol/sql/parser.go`: SQL parsing implementation
- `protocol/sql/planner.go`: Query planning and AST extraction
- `protocol/sql/optimizer.go`: Query optimization and plan rewriting
- `protocol/sql/executor.go`: Query execution against storage

### Statistics Collection (Phase 8.8)
- `catalog/stats_collector.go`: Statistics collection framework
- `protocol/sql/stats_commands.go`: ANALYZE command implementation
- `protocol/sql/cost_model.go`: Cost calculation framework (Phase 8.8.4)

### Storage Engine
- `engine/engine.go`: Storage engine interface
- `engine/pebble_engine.go`: Pebble-based implementation

### Transaction Management
- `protocol/pgserver/server.go`: Server initialization and client handling
- `protocol/sql/transaction.go`: Transaction state management

### System Catalog
- `catalog/system_tables.go`: System table implementation (pg_indexes, pg_constraint)
- `catalog/manager.go`: Catalog manager with system table registration

### DDL Parser
- `protocol/sql/ddl_parser.go`: DDL parsing implementation
- `catalog/schema_manager.go`: Schema management and validation

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