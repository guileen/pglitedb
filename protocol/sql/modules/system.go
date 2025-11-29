package modules

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// SystemTableExecutor handles system table queries
type SystemTableExecutor struct {
	catalog catalog.Manager
}

// NewSystemTableExecutor creates a new system table executor
func NewSystemTableExecutor(catalog catalog.Manager) *SystemTableExecutor {
	return &SystemTableExecutor{
		catalog: catalog,
	}
}

// ExecuteSystemTableQuery executes a system table query
func (ste *SystemTableExecutor) ExecuteSystemTableQuery(ctx context.Context, plan *sql.Plan) (*types.ResultSet, error) {
	if ste.catalog == nil {
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
	queryResult, err := ste.catalog.QuerySystemTable(ctx, fullTableName, filter)
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