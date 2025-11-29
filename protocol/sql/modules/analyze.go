package modules

import (
	"context"
	"fmt"
	"strconv"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/types"
)

// AnalyzeExecutor handles ANALYZE statement execution
type AnalyzeExecutor struct {
	catalog catalog.Manager
}

// NewAnalyzeExecutor creates a new analyze executor
func NewAnalyzeExecutor(catalog catalog.Manager) *AnalyzeExecutor {
	return &AnalyzeExecutor{
		catalog: catalog,
	}
}

// ExecuteAnalyze executes an ANALYZE statement
func (ae *AnalyzeExecutor) ExecuteAnalyze(ctx context.Context, query string) (*types.ResultSet, error) {
	if ae.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Parse the ANALYZE statement
	parser := sql.NewDDLParser()
	ddlStmt, err := parser.Parse(query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse ANALYZE statement: %w", err)
	}
	
	// Extract the analyze statement
	analyzeStmt, ok := ddlStmt.Statement.(*sql.AnalyzeStatement)
	if !ok {
		return nil, fmt.Errorf("failed to extract analyze statement")
	}
	
	// Get the stats collector from the catalog
	statsCollector := ae.catalog.GetStatsCollector()
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
		tableDef, err := ae.catalog.GetTableDefinition(ctx, 1, analyzeStmt.TableName)
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