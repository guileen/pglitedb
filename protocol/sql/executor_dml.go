package sql

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/types"
)

// =============================================================================
// DML EXECUTION METHODS
// =============================================================================

func (e *Executor) executeSelect(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Check if this is an aggregate query
	if len(plan.Aggregates) > 0 {
		return e.executeAggregateSelect(ctx, plan)
	}

	// Check if this is a function call (fields start with "func:")
	if len(plan.Fields) > 0 && strings.HasPrefix(plan.Fields[0], "func:") {
		return e.executeFunctionCall(ctx, plan)
	}

	// Handle regular table queries
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

// executeFunctionCall handles function calls in SELECT statements
func (e *Executor) executeFunctionCall(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if len(plan.Fields) == 0 {
		return nil, fmt.Errorf("no fields specified in function call")
	}

	// Extract function name (remove "func:" prefix)
	funcName := strings.TrimPrefix(plan.Fields[0], "func:")

	// Handle specific functions
	switch funcName {
	case "version":
		// Return version information
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"PGLiteDB 0.1.0 (compatible with PostgreSQL 14.0)"},
		}
		result.Count = 1
		return result, nil

	case "current_database", "current_catalog":
		// Return current database name
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"pglitedb"},
		}
		result.Count = 1
		return result, nil

	case "current_user", "user", "session_user", "current_role":
		// Return current user (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"postgres"},
		}
		result.Count = 1
		return result, nil

	case "current_schema":
		// Return current schema (placeholder)
		result := types.AcquireExecutorResultSet()
		result.Columns = []string{funcName}
		result.Rows = [][]interface{}{
			{"public"},
		}
		result.Count = 1
		return result, nil

	default:
		return nil, fmt.Errorf("function %s not implemented", funcName)
	}
}

// executeAggregateSelect handles SELECT queries with aggregate functions
func (e *Executor) executeAggregateSelect(ctx context.Context, plan *Plan) (*types.ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Handle system table queries
	if isSystemTable(plan.Table) {
		return e.executeSystemTableQuery(ctx, plan)
	}

	tenantID := e.getTenantIDFromContext(ctx)

	// Check if table exists
	_, err := e.catalog.GetTableDefinition(ctx, tenantID, plan.Table)
	if err != nil {
		return nil, fmt.Errorf("table %s not found", plan.Table)
	}

	// Prepare query options
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

	// Determine columns to fetch - include both regular fields and aggregate fields
	columns := make([]string, 0)
	columns = append(columns, plan.Fields...)
	
	// For aggregates, we need to include the underlying columns
	for _, agg := range plan.Aggregates {
		if agg.Field != "*" {
			columns = append(columns, agg.Field)
		}
	}
	
	// Include group by columns
	columns = append(columns, plan.GroupBy...)

	// Remove duplicates
	seen := make(map[string]bool)
	uniqueColumns := make([]string, 0)
	for _, col := range columns {
		if !seen[col] && col != "*" && !strings.HasPrefix(col, "func:") {
			seen[col] = true
			uniqueColumns = append(uniqueColumns, col)
		}
	}

	opts := &types.QueryOptions{
		Columns: uniqueColumns,
		OrderBy: orderByStrings,
		Limit:   limit,
		Offset:  offset,
	}

	// Execute the base query to get the data
	queryResult, err := e.catalog.Query(ctx, tenantID, plan.Table, opts)
	if err != nil {
		return nil, fmt.Errorf("query execution failed: %w", err)
	}

	// Process aggregates manually for now
	result := types.AcquireExecutorResultSet()
	
	// Set up columns - use aliases or function names
	result.Columns = make([]string, 0)
	for _, agg := range plan.Aggregates {
		if agg.Alias != "" {
			result.Columns = append(result.Columns, agg.Alias)
		} else {
			result.Columns = append(result.Columns, fmt.Sprintf("%s(%s)", strings.ToLower(agg.Function), agg.Field))
		}
	}
	
	// Add group by columns to the result
	result.Columns = append(result.Columns, plan.GroupBy...)

	// Simple aggregation logic
	if len(plan.GroupBy) == 0 {
		// Global aggregation
		count := len(queryResult.Rows)
		
		// Create result row
		row := make([]interface{}, len(result.Columns))
		row[0] = count
		
		// Add group by values if any
		for i := range plan.GroupBy {
			// For simplicity, we're not actually grouping in this basic implementation
			row[len(plan.Aggregates)+i] = nil
		}
		
		result.Rows = append(result.Rows, row)
		result.Count = 1
	} else {
		// For other cases, return a simple result
		row := make([]interface{}, len(result.Columns))
		for i := range row {
			row[i] = 0 // Default values
		}
		result.Rows = append(result.Rows, row)
		result.Count = 1
	}

	return result, nil
}

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