package modules

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// DDLExecutor handles DDL (Data Definition Language) operations
type DDLExecutor struct {
	catalog catalog.Manager
}

// NewDDLExecutor creates a new DDL executor
func NewDDLExecutor(catalog catalog.Manager) *DDLExecutor {
	return &DDLExecutor{
		catalog: catalog,
	}
}

// ExecuteDDL executes a DDL statement
func (de *DDLExecutor) ExecuteDDL(ctx context.Context, query string) (*types.ResultSet, error) {
	// Parse the DDL statement
	ddlParser := sql.NewDDLParser()
	ddlStmt, err := ddlParser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse DDL statement: %w", err)
	}

	// Handle different DDL statement types
	switch ddlStmt.Type {
	case sql.CreateTableStatement:
		return de.executeCreateTable(ctx, ddlStmt)
	case sql.CreateIndexStatement:
		return de.executeCreateIndex(ctx, ddlStmt)
	case sql.DropTableStatement:
		return de.executeDropTable(ctx, ddlStmt)
	case sql.DropIndexStatement:
		return de.executeDropIndex(ctx, ddlStmt)
	case sql.AlterTableStatement:
		return de.executeAlterTable(ctx, ddlStmt)
	case sql.CreateViewStatement:
		return de.executeCreateView(ctx, ddlStmt)
	case sql.DropViewStatement:
		return de.executeDropView(ctx, ddlStmt)
	case sql.AnalyzeStatementType:
		// This should be handled by the analyze executor
		return nil, fmt.Errorf("ANALYZE should be handled by analyze executor")
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
func (de *DDLExecutor) executeCreateTable(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
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
	if err := de.catalog.CreateTable(ctx, 1, tableDef); err != nil {
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeCreateIndex handles CREATE INDEX statements
func (de *DDLExecutor) executeCreateIndex(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
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
	if err := de.catalog.CreateIndex(ctx, 1, ddlStmt.TableName, indexDef); err != nil {
		return nil, fmt.Errorf("failed to create index: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeDropTable handles DROP TABLE statements
func (de *DDLExecutor) executeDropTable(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the table from the catalog
	err := de.catalog.DropTable(ctx, 1, ddlStmt.TableName)
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
func (de *DDLExecutor) executeDropIndex(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the index from the catalog
	if err := de.catalog.DropIndex(ctx, 1, ddlStmt.TableName, ddlStmt.IndexName); err != nil {
		return nil, fmt.Errorf("failed to drop index: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeAlterTable handles ALTER TABLE statements
func (de *DDLExecutor) executeAlterTable(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
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
func (de *DDLExecutor) executeCreateView(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Create the view in the catalog
	if err := de.catalog.CreateView(ctx, 1, ddlStmt.ViewName, ddlStmt.ViewQuery, ddlStmt.Replace); err != nil {
		return nil, fmt.Errorf("failed to create view: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// executeDropView handles DROP VIEW statements
func (de *DDLExecutor) executeDropView(ctx context.Context, ddlStmt *sql.DDLStatement) (*types.ResultSet, error) {
	if de.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Drop the view from the catalog
	if err := de.catalog.DropView(ctx, 1, ddlStmt.ViewName); err != nil {
		return nil, fmt.Errorf("failed to drop view: %w", err)
	}

	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// mapPostgreSQLTypeToInternal maps PostgreSQL type names to internal column types
func mapPostgreSQLTypeToInternal(pgType string) string {
	switch pgType {
	case "int4":
		return "integer"
	case "int2":
		return "smallint"
	case "int8":
		return "bigint"
	case "float4":
		return "real"
	case "float8":
		return "double"
	case "bool":
		return "boolean"
	case "varchar", "character varying":
		return "varchar"
	case "char", "character", "bpchar":
		return "char"
	case "timestamp", "timestamp without time zone":
		return "timestamp"
	case "decimal", "numeric":
		return "numeric"
	case "serial":
		return "serial"
	case "bigserial":
		return "bigserial"
	case "smallserial":
		return "smallserial"
	default:
		return pgType
	}
}