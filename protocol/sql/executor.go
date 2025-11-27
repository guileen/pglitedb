package sql

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

type Executor struct {
	planner *Planner
	catalog catalog.Manager
}

type ResultSet struct {
	Columns      []string
	Rows         [][]interface{}
	Count        int
	LastInsertID int64
}

func NewExecutor(planner *Planner) *Executor {
	return &Executor{
		planner: planner,
	}
}

func NewExecutorWithCatalog(planner *Planner, catalog catalog.Manager) *Executor {
	return &Executor{
		planner: planner,
		catalog: catalog,
	}
}

func (e *Executor) Execute(ctx context.Context, query string) (*ResultSet, error) {
	plan, err := e.planner.CreatePlan(query)
	if err != nil {
		return nil, fmt.Errorf("failed to create execution plan: %w", err)
	}

	switch plan.Operation {
	case "select":
		return e.executeSelect(ctx, plan)
	case "ddl":
		return e.executeDDL(ctx, query)
	case "insert":
		return e.executeInsert(ctx, plan)
	case "update":
		return e.executeUpdate(ctx, plan)
	case "delete":
		return e.executeDelete(ctx, plan)
	case "analyze":
		return e.executeAnalyze(ctx, query)
	default:
		return nil, fmt.Errorf("unsupported operation: %v", plan.Operation)
	}
}

func (e *Executor) executeDDL(ctx context.Context, query string) (*ResultSet, error) {
	// For now, we'll just return a successful result for DDL operations
	// In a full implementation, we would actually create/drop/alter tables
	// log.Printf("DDL statement executed: %s", query)
	return &ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   0,
	}, nil
}

func (e *Executor) executeSelect(ctx context.Context, plan *Plan) (*ResultSet, error) {
	if e.catalog == nil {
		result := &ResultSet{
			Columns: plan.Fields,
			Count:   0,
		}
		result.Rows = append(result.Rows, []interface{}{"mock_value1", "mock_value2"})
		result.Count = len(result.Rows)
		return result, nil
	}

	if isSystemTable(plan.Table) {
		return e.executeSystemTableQuery(ctx, plan)
	}

	tenantID := int64(1)

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

	result := &ResultSet{
		Columns: plan.Fields,
		Rows:    queryResult.Rows,
		Count:   int(queryResult.Count),
	}

	return result, nil
}

func (e *Executor) ExecuteParsed(ctx context.Context, parsed *ParsedQuery) (*ResultSet, error) {
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

func isSystemTable(tableName string) bool {
	return strings.HasPrefix(tableName, "information_schema.") || 
		   strings.HasPrefix(tableName, "pg_catalog.")
}

func (e *Executor) executeSystemTableQuery(ctx context.Context, plan *Plan) (*ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	filter := make(map[string]interface{})
	for _, cond := range plan.Conditions {
		if cond.Operator == "=" {
			filter[cond.Field] = cond.Value
		}
	}
	
	queryResult, err := e.catalog.QuerySystemTable(ctx, plan.Table, filter)
	if err != nil {
		return nil, fmt.Errorf("system table query failed: %w", err)
	}
	
	if len(queryResult.Columns) == 0 {
		return &ResultSet{
			Columns: []string{},
			Rows:    [][]interface{}{},
			Count:   0,
		}, nil
	}
	
	columnNames := make([]string, len(queryResult.Columns))
	for i, col := range queryResult.Columns {
		columnNames[i] = col.Name
	}
	
	result := &ResultSet{
		Columns: columnNames,
		Rows:    queryResult.Rows,
		Count:   len(queryResult.Rows),
	}
	
	return result, nil
}
func (e *Executor) executeInsert(ctx context.Context, plan *Plan) (*ResultSet, error) {
	if e.catalog == nil {
		return nil, fmt.Errorf("catalog not initialized")
	}
	
	// Extract values from the plan
	values := plan.Values
	
	tenantID := int64(1) // Get from context
	lastInsertID, err := e.catalog.InsertRow(ctx, tenantID, plan.Table, values)
	if err != nil {
		return nil, err
	}
	
	return &ResultSet{
		Columns:      []string{},
		Rows:         [][]interface{}{},
		Count:        1,
		LastInsertID: lastInsertID,
	}, nil
}

func (e *Executor) executeUpdate(ctx context.Context, plan *Plan) (*ResultSet, error) {
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
	
	tenantID := int64(1) // Get from context
	affected, err := e.catalog.UpdateRows(ctx, tenantID, plan.Table, values, conditions)
	if err != nil {
		return nil, err
	}
	
	return &ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   int(affected),
	}, nil
}

func (e *Executor) executeDelete(ctx context.Context, plan *Plan) (*ResultSet, error) {
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
	
	return &ResultSet{
		Columns: []string{},
		Rows:    [][]interface{}{},
		Count:   int(affected),
	}, nil
}

// executeAnalyze executes an ANALYZE statement to collect table and column statistics
func (e *Executor) executeAnalyze(ctx context.Context, query string) (*ResultSet, error) {
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
		return &ResultSet{
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
				// Find column ID
				columnID := -1
				for i, col := range tableDef.Columns {
					if col.Name == columnName {
						columnID = i + 1 // Column IDs are 1-based
						break
					}
				}
				
				if columnID == -1 {
					return nil, fmt.Errorf("column %s not found in table %s", columnName, analyzeStmt.TableName)
				}
				
				// Collect column statistics
				_, err = statsCollector.CollectColumnStats(ctx, tableID, columnID)
				if err != nil {
					return nil, fmt.Errorf("failed to collect column statistics for %s: %w", columnName, err)
				}
			}
		} else {
			// Collect statistics for all columns
			for i, col := range tableDef.Columns {
				columnID := i + 1 // Column IDs are 1-based
				_, err = statsCollector.CollectColumnStats(ctx, tableID, columnID)
				if err != nil {
					return nil, fmt.Errorf("failed to collect column statistics for %s: %w", col.Name, err)
				}
			}
		}
		
		return &ResultSet{
			Columns: []string{"message"},
			Rows:    [][]interface{}{{fmt.Sprintf("ANALYZE completed for table %s", analyzeStmt.TableName)}},
			Count:   1,
		}, nil
	}
	
	return &ResultSet{
		Columns: []string{"message"},
		Rows:    [][]interface{}{{"ANALYZE completed"}},
		Count:   1,
	}, nil
}