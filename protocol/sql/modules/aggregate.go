package modules

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// AggregateExecutor handles SELECT queries with aggregate functions
type AggregateExecutor struct {
	catalog catalog.Manager
}

// NewAggregateExecutor creates a new aggregate executor
func NewAggregateExecutor(catalog catalog.Manager) *AggregateExecutor {
	return &AggregateExecutor{
		catalog: catalog,
	}
}

// ExecuteAggregateSelect executes a SELECT query with aggregate functions
func (ae *AggregateExecutor) ExecuteAggregateSelect(ctx context.Context, plan *sql.Plan) (*types.ResultSet, error) {
	if ae.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}

	// Handle system table queries
	if isSystemTable(plan.Table) {
		return nil, fmt.Errorf("system table queries should be handled by system table executor")
	}

	tenantID := ae.getTenantIDFromContext(ctx)

	// Check if table exists
	_, err := ae.catalog.GetTableDefinition(ctx, tenantID, plan.Table)
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
	queryResult, err := ae.catalog.Query(ctx, tenantID, plan.Table, opts)
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

// getTenantIDFromContext extracts tenant ID from context, defaulting to 1 if not found
func (ae *AggregateExecutor) getTenantIDFromContext(ctx context.Context) int64 {
	if ctx == nil {
		return 1
	}
	
	if tenantID, ok := ctx.Value(logger.TenantIDKey).(int64); ok {
		return tenantID
	}
	
	// Default to 1 for backward compatibility
	return 1
}

// isSystemTable checks if a table is a system table
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