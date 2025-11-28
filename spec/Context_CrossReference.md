# Component Cross-Reference Guide

★ Core Goal: Provide clear navigation paths and relationships between related components

## Critical Infrastructure Issues Cross-Reference
❗ **Priority Fix Areas**: Cross-component issues causing system-wide failures

1. **System Table Implementation Deficiencies**
   - Primary: `catalog/system_tables.go` (pg_class, pg_attribute, pg_namespace, pg_type)
   - Secondary: `protocol/sql/planner.go` (catalog lookup mechanisms)
   - Context: `spec/Context_Catalog.md` and `spec/GUIDE.md`

2. **DDL Operation Failures**
   - Primary: `protocol/sql/ddl_parser.go` (CREATE TABLE, DROP TABLE persistence)
   - Secondary: `catalog/manager.go` (metadata storage/retrieval)
   - Context: `spec/Context_DDL.md` and `spec/GUIDE.md`

3. **Query Execution Path Issues**
   - Primary: `protocol/sql/planner.go` (system catalog lookup failures)
   - Secondary: `protocol/sql/executor.go` (query execution with metadata)
   - Context: `spec/Context_Transaction.md` and `spec/GUIDE.md`

## Phase 8.7.11 Completed Status
✅ All core components fully integrated with:
- Query processing pipeline optimized
- Catalog initialization fixed
- DDL parser implemented
- System tables extended

## Query Processing Component Relationships

### File Mapping for Common Development Tasks

1. **ORDER BY Issues**
   - Primary: `protocol/sql/planner.go` (extractSelectInfoFromPGNode method)
   - Secondary: `protocol/sql/optimizer.go` (applyRewriteRules method)
   - Test: `protocol/sql/integration_test.go` (ORDER BY test cases)
   - Context: `spec/Context_Query.md` (Query Processing Context)

2. **Catalog Initialization Problems**
   - Primary: `protocol/sql/planner.go` (NewPlannerWithCatalog, SetCatalog methods)
   - Secondary: `protocol/pgserver/server.go` (server initialization)
   - Context: `spec/Context_Transaction.md` (Transaction Management Context)

3. **Parse Failures**
   - Primary: `protocol/sql/parser.go` and `protocol/sql/pg_parser.go`
   - Secondary: `protocol/sql/planner.go` (CreatePlan method)
   - Context: `spec/Context_Query.md` (AST-Based SQL Parsing Optimization)

4. **System Table Queries**
   - Primary: `catalog/system_tables.go` (system table implementation)
   - Secondary: `protocol/sql/executor.go` (system table query execution)
   - Context: `spec/Context_Catalog.md` (System Catalog Context)

5. **DDL Parse Failures**
   - Primary: `protocol/sql/ddl_parser.go` (DDL parsing implementation)
   - Secondary: `catalog/schema_manager.go` (schema management)
   - Context: `spec/Context_DDL.md` (DDL Parser Context)

6. **Statistics Collection Issues**
   - Primary: `catalog/stats_collector.go` (statistics collection implementation)
   - Secondary: `protocol/sql/optimizer.go` (statistics usage)
   - Context: `spec/Context_Database.md` (Statistics Collection Framework)

7. **Transaction Management Issues**
   - Primary: `transaction/manager.go` (transaction coordination)
   - Secondary: `transaction/context.go` (transaction state management)
   - Context: `spec/Context_Transaction.md` (Transaction Management Context)

8. **Concurrency Control Issues**
   - Primary: `transaction/lock_manager.go` (locking implementation)
   - Secondary: `storage/mvcc.go` (MVCC implementation)
   - Context: `spec/Context_Transaction.md` (Transaction Management Context)

9. **Recovery Issues**
   - Primary: `transaction/log.go` (logging implementation)
   - Secondary: `transaction/manager.go` (recovery coordination)
   - Context: `spec/Context_Transaction.md` (Transaction Management Context)

### Data Flow Cross-References

1. **From Client to Storage**
   ```
   Client → PG Server → Parser → Planner → Optimizer → Executor → Catalog → Storage Engine
                                 ↓
                              DDL Parser (for DDL statements)
   ```

2. **Key Method Call Chains**
   - `server.handleQuery` → `planner.CreatePlan` → `optimizer.OptimizePlan` → `executor.Execute`
   - `planner.extractSelectInfoFromPGNode` → extracts all query components including ORDER BY
   - `optimizer.applyRewriteRules` → copies all plan fields to optimized plan
   - `catalog.PopulateSystemTables` → populates pg_indexes and pg_constraint with metadata
   - `ddl_parser.Parse` → `catalog.ApplySchemaChange` → metadata updates

3. **Phase 8.8 Extended Data Flows**
   - `stats_collector.CollectStats` → `optimizer.UseStatsForCosting` → cost-based optimization
   - `ddl_parser.ParseIndexDDL` → `catalog.CreateIndex` → `pg_indexes` registration

4. **Transaction Data Flow**
   - `server.handleTransactionCommand` → `transaction.Manager.Begin/Commit/Rollback` → `lock_manager.Acquire/Release` → `mvcc_storage.Get/Put` → `log_manager.WriteLog`
   - `transaction.context.CreateSavepoint` → saves transaction state
   - `transaction.manager.PerformMVCCOperation` → `mvcc_storage.VersionManagement` → consistent reads

### Common Debugging Paths

1. **When Tests Fail Due to Missing Query Components**
   - Check `planner.extractSelectInfoFromPGNode` for proper extraction
   - Verify `optimizer.applyRewriteRules` for complete field copying
   - Add debug logging to trace plan contents before/after optimization

2. **When Catalog Issues Occur**
   - Trace from `server.go` initialization through `NewPlannerWithCatalog`
   - Check `planner.SetCatalog` calls
   - Verify catalog manager is properly passed to executor

3. **When Transaction Issues Occur**
   - Trace from `server.handleTransactionCommand` through `transaction.Manager`
   - Check transaction state transitions in `transaction.context`
   - Verify lock acquisition/release in `lock_manager`
   - Monitor log entries in `log_manager`

4. **When Concurrency Issues Occur**
   - Monitor deadlock detection in `lock_manager`
   - Check MVCC version visibility in `mvcc_storage`
   - Verify isolation level enforcement in `transaction.context`

## Performance Considerations Cross-Reference

### Memory Management Across Components
- `engine/pebble_engine.go`: Buffer reuse and pre-allocation
- `protocol/sql/planner.go`: Slice pre-allocation in plan creation
- `protocol/sql/optimizer.go`: Efficient statistics collection

### Optimization Decision Points
- `protocol/sql/optimizer.go`: Uses catalog statistics for optimization decisions
- `protocol/sql/planner.go`: Early pruning and simplification
- `engine/pebble_engine.go`: Batch operation optimization

## Error Handling Cross-Reference

### Common Error Patterns and Where They're Handled
1. **Catalog Not Initialized**
   - `planner.go`: Check for nil executor
   - `executor.go`: Verify catalog availability

2. **Parse Failures**
   - `parser.go`: Syntax error handling
   - `planner.go`: AST extraction error handling

3. **Optimization Failures**
   - `optimizer.go`: Graceful degradation to original plan
   - `planner.go`: Fallback when optimization fails

4. **Transaction State Errors**
   - `transaction/context.go`: Invalid state transition handling
   - `transaction/manager.go`: Transaction lifecycle validation

5. **Concurrency Control Errors**
   - `transaction/lock_manager.go`: Deadlock detection and resolution
   - `storage/mvcc.go`: Version conflict handling

6. **Recovery Errors**
   - `transaction/log.go`: Log corruption and recovery handling
   - `transaction/manager.go`: Recovery procedure error handling

## Performance Considerations Cross-Reference

### Memory Management Across Components
- `engine/pebble_engine.go`: Buffer reuse and pre-allocation
- `protocol/sql/planner.go`: Slice pre-allocation in plan creation
- `protocol/sql/optimizer.go`: Efficient statistics collection
- `catalog/stats_collector.go`: Memory-efficient statistics sampling
- `transaction/context.go`: Efficient transaction context management
- `transaction/lock_manager.go`: Lock table memory optimization

### Optimization Decision Points
- `protocol/sql/optimizer.go`: Uses catalog statistics for optimization decisions
- `protocol/sql/planner.go`: Early pruning and simplification
- `engine/pebble_engine.go`: Batch operation optimization
- `transaction/manager.go`: Transaction scheduling and batching
- `storage/mvcc.go`: Version cleanup and garbage collection

### Phase 8.8 Performance Considerations
- `protocol/sql/cost_model.go`: Cost calculation efficiency
- `protocol/sql/join_optimizer.go`: JOIN algorithm selection performance
- `catalog/stats_collector.go`: Statistics collection overhead minimization

### Phase 9.1 Performance Considerations
- `transaction/lock_manager.go`: Lock contention reduction
- `storage/mvcc.go`: MVCC version chain traversal optimization
- `transaction/log.go`: Log write batching and flushing
- `transaction/manager.go`: Transaction pooling and reuse

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