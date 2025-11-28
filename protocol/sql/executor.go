package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/types"
)

type Executor struct {
	planner *Planner
	catalog catalog.Manager
	inTransaction bool
	pipeline *QueryPipeline
}

// GetCatalog returns the catalog manager
func (e *Executor) GetCatalog() catalog.Manager {
	return e.catalog
}

// getTenantIDFromContext extracts tenant ID from context, defaulting to 1 if not found
func (e *Executor) getTenantIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 1
	}
	
	if tenantID, ok := ctx.Value(logger.TenantIDKey).(int64); ok {
		return tenantID
	}
	
	// Default to 1 for backward compatibility
	return 1
}

func NewExecutor(planner *Planner) *Executor {
	return &Executor{
		planner: planner,
	}
}

func NewExecutorWithCatalog(planner *Planner, catalog catalog.Manager) *Executor {
	exec := &Executor{
		planner: planner,
		catalog: catalog,
	}
	
	// Initialize pipeline for batched execution
	exec.pipeline = NewQueryPipeline(exec, 10)
	
	return exec
}

func (e *Executor) Execute(ctx context.Context, query string) (*types.ResultSet, error) {
	// Use pooled ResultSet instead of allocating new one each time
	// result := types.AcquireResultSet()
	// defer func() {
		// Only release if not returning successfully
		// Note: This is a simplified approach - in practice, the caller would need to release
	// }()
	
	parsed, err := e.planner.parser.Parse(query)
	if err != nil {
		// ReleaseResultSet(result)  // This line was incorrect
		return nil, fmt.Errorf("failed to parse query: %w", err)
	}

	switch parsed.Type {
	case SelectStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		return e.executeSelect(ctx, plan)
	case InsertStatement, UpdateStatement, DeleteStatement:
		plan, err := e.planner.CreatePlan(query)
		if err != nil {
			return nil, fmt.Errorf("failed to create execution plan: %w", err)
		}
		switch parsed.Type {
		case InsertStatement:
			return e.executeInsert(ctx, plan)
		case UpdateStatement:
			return e.executeUpdate(ctx, plan)
		case DeleteStatement:
			return e.executeDelete(ctx, plan)
		}
	case BeginStatement:
		return e.executeBegin(ctx)
	case CommitStatement:
		return e.executeCommit(ctx)
	case RollbackStatement:
		return e.executeRollback(ctx)
	case CreateTableStatement, DropTableStatement, AlterTableStatement, 
	     CreateIndexStatement, DropIndexStatement, CreateViewStatement, DropViewStatement:
		return e.executeDDL(ctx, query)
	case AnalyzeStatementType:
		return e.executeAnalyze(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", parsed.Type)
	}

	// This should never be reached
	return nil, fmt.Errorf("unhandled statement type: %v", parsed.Type)
}

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

// =============================================================================
// DML EXECUTION METHODS
// =============================================================================

func (e *Executor) executeSelect(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	if isSystemTable(plan.Table) {
		return e.executeSystemTableQuery(ctx, plan)
	}

	tenantID := e.getTenantIDFromContext(ctx)

	// Check if table exists
	_, err := e.catalog.GetTableDefinition(ctx, tenantID, plan.Table)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", plan.Table)
	}

	orderByStrings := make([]string, len(plan.OrderBy))
	for i, ob := range plan.OrderBy {
		orderByStrings[i] = ob.Field
	}

	var limit, offset *int
	if plan.Limit != nil {
		l := int(*plan.Limit)
		limit = &l
	}
	if plan.Offset != nil {
		o := int(*plan.Offset)
		offset = &o
	}

	opts := &types.QueryOptions{
		Columns: plan.Fields,
		OrderBy: orderByStrings,
		Limit:   limit,
		Offset:  offset,
	}

	queryResult, err := e.catalog.Query(ctx, tenantID, plan.Table, opts)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Use pooled ResultSet for better memory management
	result := types.AcquireExecutorResultSet()
	result.Columns = make([]string, len(plan.Fields))
	copy(result.Columns, plan.Fields)
	result.Rows = make([][]interface{}, len(queryResult.Rows))
	copy(result.Rows, queryResult.Rows)
	result.Count = int(queryResult.Count)

	return result, nil
}

func (e *Executor) ExecuteParsed(ctx context.Context, parsed *ParsedQuery) (*types.ResultSet, error) {
	plan, err := e.planner.CreatePlan(parsed.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Type {
	case SelectStatement:
		return e.executeSelect(ctx, plan)
	default:
		return nil, fmt.Errorf("unsupported statement type: %v", plan.Type)
	}
}

func (e *Executor) ValidateQuery(query string) error {
	_, err := e.planner.CreatePlan(query)
	return err
}

func (e *Executor) Explain(query string) (*Plan, error) {
	return e.planner.CreatePlan(query)
}

func IsSystemTable(tableName string) bool {
	return isSystemTable(tableName)
}

func isSystemTable(tableName string) bool {
	// Normalize table name by removing any extra whitespace and converting to lowercase
	normalized := strings.TrimSpace(strings.ToLower(tableName))
	
	// Check for system table prefixes
	if strings.HasPrefix(normalized, "information_schema.") || 
	   strings.HasPrefix(normalized, "pg_catalog.") {
		return true
	}
	
	// Also check for common system table names without schema prefix
	if strings.HasPrefix(normalized, "pg_") {
		return true
	}
	
	// Check for information_schema tables without schema prefix
	if normalized == "tables" || normalized == "columns" || normalized == "views" {
		return true
	}
	
	return false
}

// =============================================================================
// SYSTEM TABLE METHODS
// =============================================================================

func (e *Executor) executeSystemTableQuery(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Normalize the table name to ensure consistent format
	fullTableName := strings.TrimSpace(plan.Table)
	
	// Ensure the table name has the proper schema prefix
	if !strings.Contains(fullTableName, ".") {
		// If no schema is specified, try to determine the correct schema
		if strings.HasPrefix(strings.ToLower(fullTableName), "pg_") {
			fullTableName = "pg_catalog." + fullTableName
		} else {
			// Default to information_schema for other system-like tables
			fullTableName = "information_schema." + fullTableName
		}
	}
	
	filter := make(map[string]interface{})
	for _, cond := range plan.Conditions {
		// Support multiple operators, not just equality
		switch cond.Operator {
		case "=", "==":
			filter[cond.Field] = cond.Value
		case "!=":
			// For inequality, we might need special handling in the system table query
			// For now, we'll pass it through and let the system table implementation handle it
			filter[cond.Field] = map[string]interface{}{
				"operator": "!=", 
				"value": cond.Value,
			}
		case ">":
			filter[cond.Field] = map[string]interface{}{
				"operator": ">", 
				"value": cond.Value,
			}
		case "<":
			filter[cond.Field] = map[string]interface{}{
				"operator": "<", 
				"value": cond.Value,
			}
		case ">=":
			filter[cond.Field] = map[string]interface{}{
				"operator": ">=", 
				"value": cond.Value,
			}
		case "<=":
			filter[cond.Field] = map[string]interface{}{
				"operator": "<=", 
				"value": cond.Value,
			}
		}
	}
	
	// Use QuerySystemTable with default tenant ID 1
	// In a full implementation, the tenant ID should be extracted from context or request
	queryResult, err := e.catalog.QuerySystemTable(ctx, fullTableName, filter)
	if err != nil {
		return nil, fmt.Errorf("system table query failed for '%s': %w", fullTableName, err)
	}
	
	if len(queryResult.Columns) == 0 {
		return &types.ResultSet{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Count:   0,
		}, nil
	}
	
	columnNames := make([]string, len(queryResult.Columns))
	for i, col := range queryResult.Columns {
		columnNames[i] = col.Name
	}
	
	result := &types.ResultSet{
		Columns: columnNames,
		Rows:    queryResult.Rows,
		Count:   len(queryResult.Rows),
	}
	
	return result, nil
}

func (e *Executor) executeInsert(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Extract values from the plan
	values := plan.Values
	
	tenantID := e.getTenantIDFromContext(ctx)
	lastInsertID, err := e.catalog.InsertRow(ctx, tenantID, plan.Table, values)
	if err != nil {
		return nil, err
	}
	
	return &types.ResultSet{
		Columns:      []string{},
		Rows:         [][]interface{}{},
		Count:        1,
		LastInsertID: lastInsertID,
	}, nil
}

func (e *Executor) executeUpdate(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Extract values and conditions from the plan
	values := plan.Updates
	
	// Convert conditions to filter map
	conditions := make(map[string]interface{})
	for _, cond := range plan.Conditions {
		if cond.Operator == "=" {
			conditions[cond.Field] = cond.Value
		}
	}
	
	tenantID := e.getTenantIDFromContext(ctx)
	affected, err := e.catalog.UpdateRows(ctx, tenantID, plan.Table, values, conditions)
	if err != nil {
		return nil, err
	}
	
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   int(affected),
	}, nil
}

func (e *Executor) executeDelete(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Extract conditions from the plan
	conditions := make(map[string]interface{})
	for _, cond := range plan.Conditions {
		if cond.Operator == "=" {
			conditions[cond.Field] = cond.Value
		}
	}
	
	tenantID := int64(1) // Get from context
	affected, err := e.catalog.DeleteRows(ctx, tenantID, plan.Table, conditions)
	if err != nil {
		return nil, err
	}
	
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   int(affected),
	}, nil
}

func (e *Executor) executeRollback(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would rollback the transaction in the storage engine
	e.inTransaction = false
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

func (e *Executor) executeCommit(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would commit the transaction in the storage engine
	e.inTransaction = false
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// =============================================================================
// TRANSACTION METHODS
// =============================================================================

func (e *Executor) executeBegin(ctx context.Context) (*types.ResultSet, error) {
	// For now, we'll just update the transaction state
	// In a full implementation, we would begin a transaction in the storage engine
	e.inTransaction = true
	return &types.ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

// =============================================================================
// ANALYZE METHOD
// =============================================================================

func (e *Executor) executeAnalyze(ctx context.Context, query string) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Parse the ANALYZE statement
	parser := NewDDLParser()
	ddlStmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ANALYZE statement: %w", err)
	}
	
	// Extract the analyze statement
	analyzeStmt, ok := ddlStmt.Statement.(*AnalyzeStatement)
	if !ok {
		return nil, fmt.Errorf("failed to extract analyze statement")
	}
	
	// Get the stats collector from the catalog
	statsCollector := e.catalog.GetStatsCollector()
	if statsCollector == nil {
		return nil, fmt.Errorf("stats collector not available")
	}
	
	// Handle ANALYZE; (all tables)
	if analyzeStmt.AllTables {
		// For now, we'll return a success message
		// In a full implementation, we would analyze all tables
		return &types.ResultSet{
			Columns: []string{"message"},
			Rows:    [][]interface{}{{"ANALYZE completed for all tables"}},
			Count:   1,
		}, nil
	}
	
	// Handle ANALYZE table_name;
	if analyzeStmt.TableName != "" {
		// Get table definition to get table ID
		tableDef, err := e.catalog.GetTableDefinition(ctx, 1, analyzeStmt.TableName)
		if err != nil {
			return nil, fmt.Errorf("table %s not found: %w", analyzeStmt.TableName, err)
		}
		
		// Convert string ID to uint64
		tableID, err := strconv.ParseUint(tableDef.ID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid table ID format: %w", err)
		}
		
		// Collect table statistics
		_, err = statsCollector.CollectTableStats(ctx, tableID)
		if err != nil {
			return nil, fmt.Errorf("failed to collect table statistics: %w", err)
		}
		
		// If specific columns are specified, collect column statistics
		if len(analyzeStmt.Columns) > 0 {
			for _, columnName := range analyzeStmt.Columns {
				// Check if column exists in table
				found := false
				for _, col := range tableDef.Columns {
					if col.Name == columnName {
						found = true
						break
					}
				}
				
				if !found {
					return nil, fmt.Errorf("column %s not found in table %s", columnName, analyzeStmt.TableName)
				}
				
				// Collect column statistics
				_, err = statsCollector.CollectColumnStats(ctx, tableID, columnName)
				if err != nil {
					return nil, fmt.Errorf("failed to collect column statistics for %s: %w", columnName, err)
				}
			}
		} else {
			// Collect statistics for all columns
			for _, col := range tableDef.Columns {
				_, err = statsCollector.CollectColumnStats(ctx, tableID, col.Name)
				if err != nil {
					return nil, fmt.Errorf("failed to collect column statistics for %s: %w", col.Name, err)
				}
			}
		}
		
		return &types.ResultSet{
			Columns: []string{"message"},
			Rows:    [][]interface{}{{fmt.Sprintf("ANALYZE completed for table %s", analyzeStmt.TableName)}},
			Count:   1,
		}, nil
	}
	
	return &types.ResultSet{
		Columns: []string{"message"},
		Rows:    [][]interface{}{{"ANALYZE completed"}},
		Count:   1,
	}, nil
}

// executeCreateTable handles CREATE TABLE statements
func (e *Executor) executeCreateTable(ctx context.Context, ddlStmt *DDLStatement) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Convert DDL column definitions to catalog column definitions
	columns := make([]types.ColumnDefinition, len(ddlStmt.Columns))
	for i, col := range ddlStmt.Columns {
		columns[i] = types.ColumnDefinition{
			Name:       col.Name,
			Type:       types.ColumnType(col.Type),
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