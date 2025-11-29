package sql

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/types"
)

// =============================================================================
// DDL EXECUTION METHODS
// =============================================================================

func (e *Executor) executeDDL(ctx context.Context, query string) (*types.ResultSet, error) {
	// Parse the DDL statement
	ddlParser := NewDDLParser()
	ddlStmt, err := ddlParser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL statement: %w", err)
	}

	// Handle different DDL statement types
	switch ddlStmt.Type {
	case CreateTableStatement:
		return e.executeCreateTable(ctx, ddlStmt)
	case CreateIndexStatement:
		return e.executeCreateIndex(ctx, ddlStmt)
	case DropTableStatement:
		return e.executeDropTable(ctx, ddlStmt)
	case DropIndexStatement:
		return e.executeDropIndex(ctx, ddlStmt)
	case AlterTableStatement:
		return e.executeAlterTable(ctx, ddlStmt)
	case CreateViewStatement:
		return e.executeCreateView(ctx, ddlStmt)
	case DropViewStatement:
		return e.executeDropView(ctx, ddlStmt)
	case AnalyzeStatementType:
		return e.executeAnalyze(ctx, query)
	default:
		// For unsupported DDL operations, return a successful result
		return &types.ResultSet{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Count:   0,
		}, nil
	}
}

// executeCreateTable handles CREATE TABLE statements
func (e *Executor) executeCreateTable(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Convert DDL column definitions to catalog column definitions
	columns := make([]types.ColumnDefinition, len(ddlStmt.Columns))
	for i, col := range ddlStmt.Columns {
		// Map PostgreSQL type names to our internal type names
		mappedType := mapPostgreSQLTypeToInternal(col.Type)
		columns[i] = types.ColumnDefinition{
			Name:       col.Name,
			Type:       types.ColumnType(mappedType),
			Nullable:   !col.NotNull,
			PrimaryKey: col.PrimaryKey,
			Unique:     col.Unique,
		}
		// Handle default values if present
		if col.Default != "" {
			// For now, we'll just store the default as a string
			// In a full implementation, we would parse and validate the default expression
			defaultValue := &types.Value{
				Data: col.Default,
				Type: types.ColumnTypeString,
			}
			columns[i].Default = defaultValue
		}
	}

	// Create table definition
	tableDef := &types.TableDefinition{
		Name:    ddlStmt.TableName,
		Columns: columns,
	}

	// Create the table in the catalog
	if err := e.catalog.CreateTable(ctx, 1, tableDef); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeCreateIndex handles CREATE INDEX statements
func (e *Executor) executeCreateIndex(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Create index definition
	indexDef := &types.IndexDefinition{
		Name:    ddlStmt.IndexName,
		Columns: ddlStmt.IndexColumns,
		Unique:  ddlStmt.Unique,
		Type:    ddlStmt.IndexType,
	}

	// Create the index in the catalog
	if err := e.catalog.CreateIndex(ctx, 1, ddlStmt.TableName, indexDef); err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeDropTable handles DROP TABLE statements
func (e *Executor) executeDropTable(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the table from the catalog
	err := e.catalog.DropTable(ctx, 1, ddlStmt.TableName)
	if err != nil {
		// If IF EXISTS is specified, ignore table not found errors
		if ddlStmt.IfExists && err == types.ErrTableNotFound {
			// Table doesn't exist, but IF EXISTS was specified, so this is not an error
			return &types.ResultSet{
				Columns: []string{},
				Rows:    [][]interface{}{},
				Count:   0,
			}, nil
		}
		return nil, fmt.Errorf("failed to drop table: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeDropIndex handles DROP INDEX statements
func (e *Executor) executeDropIndex(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the index from the catalog
	if err := e.catalog.DropIndex(ctx, 1, ddlStmt.TableName, ddlStmt.IndexName); err != nil {
		return nil, fmt.Errorf("failed to drop index: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeAlterTable handles ALTER TABLE statements
func (e *Executor) executeAlterTable(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// For now, we'll just return a successful result
	// In a full implementation, we would handle the ALTER TABLE commands
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeCreateView handles CREATE VIEW statements
func (e *Executor) executeCreateView(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Create the view in the catalog
	if err := e.catalog.CreateView(ctx, 1, ddlStmt.ViewName, ddlStmt.ViewQuery, ddlStmt.Replace); err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeDropView handles DROP VIEW statements
func (e *Executor) executeDropView(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the view from the catalog
	if err := e.catalog.DropView(ctx, 1, ddlStmt.ViewName); err != nil {
		return nil, fmt.Errorf("failed to drop view: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}