# SQL Parser Evaluation for PGLiteDB

## Overview

We evaluated `github.com/pganalyze/pg_query_go/v6` as a potential replacement for our current SQL parsing implementation. This library is a Go wrapper around the PostgreSQL query parser, which gives it several advantages over our current approach.

## Key Findings

### 1. Comprehensive PostgreSQL Compatibility
The library successfully parses all PostgreSQL constructs we tested, including:
- Basic DML operations (SELECT, INSERT, UPDATE, DELETE)
- DDL statements (CREATE TABLE, ALTER TABLE, CREATE INDEX)
- Advanced features (RETURNING clauses, CTEs, window functions)
- Complex expressions (CASE, JSON operators, array constructors)
- Subqueries (EXISTS, IN, scalar subqueries)
- PostgreSQL-specific data types and operators

### 2. Proper Placeholder Support
Unlike our previous attempts with other parsers, this library correctly handles PostgreSQL-style `$1, $2, ...` placeholders without requiring preprocessing.

### 3. Full RETURNING Clause Support
The parser correctly recognizes and structures RETURNING clauses in INSERT, UPDATE, and DELETE statements, making it ideal for our needs.

### 4. Rich AST Representation
The library provides a complete Abstract Syntax Tree (AST) representation that allows us to:
- Extract table names and column references
- Identify query types (SELECT, INSERT, etc.)
- Access WHERE conditions, ORDER BY clauses, and LIMIT/OFFSET values
- Navigate complex query structures (joins, subqueries, CTEs)

## Advantages Over Current Implementation

### Current Issues Addressed:
1. **Simple Query Protocol preprocessing** - Not needed with this parser
2. **Extended Query placeholder errors** - Parser natively supports `$1, $2, ...` syntax
3. **RETURNING clause extraction** - Directly accessible in the AST
4. **Complex PostgreSQL constructs** - Fully supported

### Technical Benefits:
1. **Production-ready** - Used by pganalyze in production systems
2. **Up-to-date** - Based on recent PostgreSQL versions
3. **Well-maintained** - Active development and community support
4. **Accurate parsing** - Uses the actual PostgreSQL parser code

## Implementation Plan

### Phase 1: Parser Replacement
1. Replace `xwb1989/sqlparser` with `pganalyze/pg_query_go/v6`
2. Update `MySQLParser.PreprocessPostgreSQLDDL()` to be a no-op for PostgreSQL queries
3. Modify `PostgreSQLParser` to use the new library
4. Update AST traversal code to use the new protobuf structures

### Phase 2: Protocol Handler Updates
1. Remove preprocessing from `handleSimpleQuery`
2. Ensure `handleParse` doesn't call the parser (as it should only store the query)
3. Update `handleExecute` to properly substitute parameters

### Phase 3: RETURNING Implementation
1. Enhance AST traversal to properly extract RETURNING clauses
2. Update result construction to handle RETURNING results
3. Implement proper LastInsertID handling through catalog operations

## Sample Usage

```go
import pg_query "github.com/pganalyze/pg_query_go/v6"

// Parse a query with placeholders and RETURNING
sql := "INSERT INTO users (name, email) VALUES ($1, $2) RETURNING id"
result, err := pg_query.Parse(sql)
if err != nil {
    // Handle error
}

// Extract RETURNING columns
insertStmt := result.Stmts[0].Stmt.GetInsertStmt()
returningList := insertStmt.GetReturningList()
// Process returningList to get column names
```

## Recommendation

We recommend adopting `github.com/pganalyze/pg_query_go/v6` as our primary SQL parser for the following reasons:

1. **Full PostgreSQL compatibility** - Eliminates parsing corner cases
2. **Native placeholder support** - Solves our current Extended Query protocol issues
3. **Proper RETURNING support** - Enables complete implementation of this feature
4. **Robust AST** - Provides rich information for query planning
5. **Production proven** - Used in commercial PostgreSQL monitoring tools

This change will significantly improve our PostgreSQL compatibility and resolve the parsing issues that have been blocking progress on several key features.