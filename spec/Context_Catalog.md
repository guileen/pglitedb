# System Catalog Context

★ Core Goal: Document system catalog implementation and management for PGliteDB

This file provides context about the system catalog implementation, including system tables like pg_indexes and pg_constraint, and their integration with the database engine.

## OID Generation Consistency
⚠️ **Critical Implementation Principle**: Consistent OID generation is essential for maintaining referential integrity between system tables. All OIDs must be generated deterministically to ensure consistency across system restarts and distributed environments.

### Key OID Generation Functions:
- `generateDeterministicOID()` - Base function for generating deterministic OIDs
- `generateTableOID()` - Generates OIDs for tables (used in pg_class.oid)
- `generateTypeOID()` - Generates OIDs for types (used in pg_type.oid)
- `generateNamespaceOID()` - Generates OIDs for namespaces (used in pg_namespace.oid)

Weight: ★★★★★ (Critical for system table integrity)

## Critical Infrastructure Issue
❗ **Priority 1 Fix Required**: System table implementation deficiencies causing "table not found" errors in all benchmark tests
- Missing or incomplete core PostgreSQL system tables: pg_class, pg_attribute, pg_namespace, pg_type
- Impact: Prevents basic database operations requiring system catalog lookups
- See master plan in [GUIDE.md](./GUIDE.md) for detailed fix approach

## System Table Relationships
⚠️ **Complex Interdependencies**: System tables have intricate relationships that must be maintained for proper database functionality:

1. **pg_class ↔ pg_attribute Relationship**
   - `pg_class.oid` references table identifiers
   - `pg_attribute.attrelid` references `pg_class.oid` 
   - Weight: ★★★★★ (Critical relationship for table metadata)

2. **pg_class ↔ pg_type Relationship**
   - `pg_class.reltype` references `pg_type.oid` for table row types
   - Weight: ★★★★☆ (Important for type system integration)

3. **pg_attribute ↔ pg_type Relationship**
   - `pg_attribute.atttypid` references `pg_type.oid` for column types
   - Weight: ★★★★★ (Critical for column type resolution)

4. **pg_class ↔ pg_namespace Relationship**
   - `pg_class.relnamespace` references `pg_namespace.oid`
   - Weight: ★★★★☆ (Important for schema qualification)

## Recent Fixes Validation
✅ **Fully Resolved**: System table implementation issues have been successfully fixed and validated
- Complete pg_class implementation with proper OID generation and referential integrity
- Complete pg_attribute implementation with correct attrelid references to pg_class.oid
- Complete pg_namespace implementation with standard namespaces and deterministic OID generation
- Complete pg_type implementation with proper type definitions and reltype integration
- See validation report in [SYSTEM_TABLE_FIXES_VALIDATION.md](./SYSTEM_TABLE_FIXES_VALIDATION.md)

## System Table Implementation Best Practices
⚠️ **Key Implementation Principles**:

1. **Consistent OID Generation**
   - Use deterministic OID functions for all system table entries
   - Ensure OID consistency across system restarts
   - Weight: ★★★★★ (Critical for referential integrity)

2. **Maintain Referential Integrity**
   - Always validate foreign key relationships between system tables
   - Ensure attrelid references valid pg_class.oid values
   - Weight: ★★★★★ (Essential for metadata consistency)

3. **Complete Schema Implementation**
   - Implement all required PostgreSQL system table columns
   - Maintain column ordering consistent with PostgreSQL specifications
   - Weight: ★★★★☆ (Important for compatibility)

## Phase 8.7.11 Completed Status
✅ System catalog fully functional with:
- pg_indexes system table implementation
- pg_constraint system table implementation
- Complete pg_class, pg_attribute, pg_namespace, pg_type implementations with consistent OID generation
- Full integration with query processing pipeline
- PostgreSQL-compatible metadata queries with proper system table relationships

For detailed implementation approach, see [GUIDE.md](./GUIDE.md) and [System Tables Enhancement Guide](./GUIDE_SYSTEM_TABLES_ENHANCEMENT.md)

## Key Architectural Components

## Phase 8.8 Planned Extensions

### Extended System Tables (8.8.3)
**Goal**: Implement comprehensive PostgreSQL-compatible system tables
**Key Components**:
1. **pg_stat_* Series** - Statistics collection system tables
   - `pg_stat_user_tables` - Table access statistics
   - `pg_stat_user_indexes` - Index access statistics
   - `pg_stats` - Column-level statistics
2. **Metadata System Tables** - Advanced schema information
   - `pg_index` - Index metadata and configuration
   - `pg_inherits` - Table inheritance relationships
   - `pg_class` - Object metadata (tables, indexes, sequences)
3. **Integration Points**:
   - `catalog/system_tables.go` - Extended system table implementations
   - `catalog/stats_collector.go` - Statistics data source integration

**Implementation Guide**: See `spec/GUIDE_SYSTEM_TABLES_ENHANCEMENT.md`

### System Tables Implementation (catalog/system_tables.go)

1. **pg_indexes System Table**
   - Implements PostgreSQL-compatible pg_indexes system table
   - Provides information about indexes in the database
   - Contains columns: schemaname, tablename, indexname, tablespace, indexdef
   - Weight: ★★★★☆ (Important for database introspection)

2. **pg_constraint System Table**
   - Implements PostgreSQL-compatible pg_constraint system table
   - Provides information about constraints in the database
   - Contains columns: conname, connamespace, contype, condeferrable, condeferred, convalidated, conrelid, contypid, confrelid, confupdtype, confdeltype, confmatchtype, conislocal, coninhcount, connoinherit, conkey, confkey, consrc
   - Weight: ★★★★☆ (Important for constraint management)

3. **Core System Tables (pg_class, pg_attribute, pg_namespace, pg_type)**
   - Implements complete PostgreSQL-compatible core system tables
   - Maintains referential integrity between tables through consistent OID generation
   - Provides complete metadata for database objects and their relationships
   - Weight: ★★★★★ (Critical for database functionality)

### Large File Refactoring Plans
⚠️ **Technical Debt Reduction Priority**: The `catalog/system_tables.go` file (1844 lines) requires refactoring to improve maintainability and code organization.

**Refactoring Strategy**:
1. **Modularization** - Split large functions into smaller, focused units
2. **Package Restructuring** - Move related functionality into separate packages
3. **Interface Abstraction** - Define clear interfaces for system table operations
4. **Test Coverage** - Ensure comprehensive test coverage during refactoring

**Current Approach**:
- Enhanced logging has been implemented to improve observability during the refactoring process
- Refactoring is being done incrementally to maintain system stability
- See `spec/REFACTORING_CHECKLIST.md` for detailed refactoring tasks and progress tracking

### Catalog Manager Integration

1. **System Table Registration**
   - System tables registered with catalog manager during initialization
   - Integrated with existing table metadata management
   - Supports standard SQL queries against system tables
   - Weight: ★★★★★ (Critical for system functionality)

2. **Data Population Mechanism**
   - System table data populated from actual database metadata
   - Real-time reflection of database structure
   - Consistent with PostgreSQL system catalog behavior
   - Weight: ★★★★☆ (Important for data accuracy)

## Component Interaction Documentation

### System Table Data Flow

1. **Catalog Initialization**
   ```
   Server Startup → Catalog Manager Init → Register System Tables → Populate from Metadata
   ```

2. **Query Processing Flow**
   ```
   SQL Query → Parser → Planner → Executor → Catalog Lookup → System Table Data
   ```

### Key Method Interactions

1. **System Table Registration**
   - `catalog.RegisterSystemTable()` registers pg_indexes and pg_constraint
   - Called during catalog manager initialization
   - Provides schema definition for system tables

2. **Data Population**
   - `catalog.PopulateSystemTables()` fills system tables with actual data
   - Called when system tables are queried
   - Reflects current database state

## Implementation Details

### pg_indexes Implementation

1. **Schema Definition**
   - Columns match PostgreSQL pg_indexes system table
   - Provides index metadata for database introspection
   - Supports standard queries like `SELECT * FROM pg_indexes`

2. **Data Source**
   - Populated from actual index definitions in catalog
   - Real-time data reflecting current index state
   - Index definition formatted as CREATE INDEX statement

### pg_constraint Implementation

1. **Schema Definition**
   - Columns match PostgreSQL pg_constraint system table
   - Provides constraint metadata for database introspection
   - Supports various constraint types (primary key, foreign key, check, unique)

2. **Data Source**
   - Populated from actual constraint definitions in catalog
   - Real-time data reflecting current constraint state
   - Constraint definition formatted appropriately for each type

## Troubleshooting Guide

### Common Issues and Solutions

1. **System Table Query Failures**
   - **Symptom**: Queries against pg_indexes or pg_constraint fail
   - **Cause**: System tables not properly registered or populated
   - **Solution**: Verify catalog initialization and system table registration

2. **Missing System Table Data**
   - **Symptom**: System tables return empty results
   - **Cause**: Data population mechanism not working correctly
   - **Solution**: Check PopulateSystemTables implementation and metadata access

3. **Inconsistent System Table Data**
   - **Symptom**: System table data doesn't match actual database state
   - **Cause**: Stale data or synchronization issues
   - **Solution**: Verify data population triggers and refresh mechanisms

## Important Context About Changes

### Why These System Tables Were Implemented

1. **PostgreSQL Compatibility**
   - Required for PostgreSQL-compatible database introspection
   - Enables standard tools and applications to work with PGliteDB
   - Supports INFORMATION_SCHEMA and other standard metadata queries
   - Weight: ★★★★★ (Critical for compatibility)

2. **Database Administration**
   - Essential for database administration and monitoring
   - Enables querying database structure and constraints
   - Supports backup, restore, and migration tools
   - Weight: ★★★★☆ (Important for operational use)

### How They Improve the System

1. **Enhanced Compatibility**
   - Full PostgreSQL system catalog compatibility
   - Standard SQL introspection capabilities
   - Support for PostgreSQL administration tools
   - Weight: ★★★★★ (Fundamental compatibility improvement)

2. **Better Observability**
   - Real-time visibility into database structure
   - Detailed constraint and index information
   - Standardized metadata access patterns
   - Weight: ★★★★☆ (Important operational improvement)

## Implementation Patterns and Best Practices

### System Table Design
When implementing new system tables, follow these patterns:
```go
// Define system table schema
systemTable := &SystemTable{
    Name: "pg_new_table",
    Columns: []Column{
        {Name: "column1", Type: "text"},
        {Name: "column2", Type: "int4"},
    },
}

// Register with catalog
catalog.RegisterSystemTable(systemTable)
```

### Data Population
Ensure system table data is populated consistently:
```go
// Populate from actual metadata
func populateSystemTable() error {
    // Get actual database metadata
    // Transform to system table format
    // Return populated data
}
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