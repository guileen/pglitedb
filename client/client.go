package client

import (
	"context"
	"fmt"
	"log"
	"strings"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/storage"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

// Client provides a unified interface for interacting with the database
// It can be used both for embedded access and for connecting to a remote server
type Client struct {
	executor *sql.Executor
	planner  *sql.Planner
}

// NewClient creates a new embedded client
func NewClient(dbPath string) *Client {
	// Create a pebble KV store
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create pebble kv: %v", err))
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	
	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}
	
	// Create SQL parser and planner with catalog
	parser := sql.NewPGParser()
	planner := sql.NewPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	
	return &Client{
		executor: exec,
		planner:  planner,
	}
}

// NewClientWithExecutor creates a new client with a custom executor
func NewClientWithExecutor(exec *sql.Executor, planner *sql.Planner) *Client {
	return &Client{
		executor: exec,
		planner:  planner,
	}
}

// convertExternalToInternalOptions converts external client.QueryOptions to internal types.QueryOptions
func convertExternalToInternalOptions(options *types.QueryOptions) *types.QueryOptions {
	// Since we're using unified types, we can return the same object
	// or create a copy if needed
	if options == nil {
		return &types.QueryOptions{}
	}
	
	// Return the same object since types are unified
	return options
}

// Query executes a query and returns the result
func (c *Client) Query(ctx context.Context, query interface{}) (*types.QueryResult, error) {
	// Convert query interface to string
	sqlQuery, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("invalid query type: expected string")
	}
	
	resultSet, err := c.executor.Execute(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert ResultSet to QueryResult
	columns := make([]types.ColumnInfo, len(resultSet.Columns))
	for i, col := range resultSet.Columns {
		columns[i] = types.ColumnInfo{
			Name: col,
			Type: types.ColumnTypeString, // Placeholder type
		}
	}
	
	return &types.QueryResult{
		Rows:         resultSet.Rows,
		Columns:      columns,
		Count:        int64(resultSet.Count),
		LastInsertID: resultSet.LastInsertID,
	}, nil
}

// Explain generates an execution plan for a query without executing it
func (c *Client) Explain(ctx context.Context, query interface{}) (interface{}, error) {
	// Convert query interface to string
	sqlQuery, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("invalid query type: expected string")
	}
	
	plan, err := c.planner.CreatePlan(sqlQuery)
	if err != nil {
		return nil, err
	}
	
	return plan, nil
}

// Insert inserts a new record into the specified table
func (c *Client) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.QueryResult, error) {
	// Convert data map to SQL INSERT statement
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("INSERT INTO %s (", tableName))
	
	columns := make([]string, 0, len(data))
	values := make([]string, 0, len(data))
	
	for column, value := range data {
		columns = append(columns, column)
		switch v := value.(type) {
		case string:
			values = append(values, fmt.Sprintf("'%s'", v))
		case int, int32, int64:
			values = append(values, fmt.Sprintf("%v", v))
		case float32, float64:
			values = append(values, fmt.Sprintf("%v", v))
		case bool:
			if v {
				values = append(values, "true")
			} else {
				values = append(values, "false")
			}
		default:
			values = append(values, fmt.Sprintf("'%v'", v))
		}
	}
	
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES (")
	sb.WriteString(strings.Join(values, ", "))
	sb.WriteString(")")
	
	sqlQuery := sb.String()
	
	resultSet, err := c.executor.Execute(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert ResultSet to QueryResult
	columnsInfo := make([]types.ColumnInfo, len(resultSet.Columns))
	for i, col := range resultSet.Columns {
		columnsInfo[i] = types.ColumnInfo{
			Name: col,
			Type: types.ColumnTypeString, // Placeholder type
		}
	}
	
	return &types.QueryResult{
		Rows:         resultSet.Rows,
		Columns:      columnsInfo,
		Count:        int64(resultSet.Count),
		LastInsertID: resultSet.LastInsertID,
	}, nil
}

// Select retrieves records from the specified table
func (c *Client) Select(ctx context.Context, tenantID int64, tableName string, options *types.QueryOptions) (*types.QueryResult, error) {
	internalOptions := convertExternalToInternalOptions(options)
	
	var sb strings.Builder
	sb.WriteString("SELECT ")
	
	if len(internalOptions.Columns) == 0 {
		sb.WriteString("*")
	} else {
		sb.WriteString(strings.Join(internalOptions.Columns, ", "))
	}
	
	sb.WriteString(fmt.Sprintf(" FROM %s", tableName))
	
	// Convert where conditions to SQL WHERE clause
	if internalOptions.Where != nil && len(internalOptions.Where) > 0 {
		sb.WriteString(" WHERE ")
		whereClauses := make([]string, 0, len(internalOptions.Where))
		for column, value := range internalOptions.Where {
			switch v := value.(type) {
			case string:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", column, v))
			case int, int32, int64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case float32, float64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case bool:
				if v {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = true", column))
				} else {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = false", column))
				}
			default:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", column, v))
			}
		}
		sb.WriteString(strings.Join(whereClauses, " AND "))
	}
	
	// Add ORDER BY clause
	if internalOptions.OrderBy != nil && len(internalOptions.OrderBy) > 0 {
		sb.WriteString(" ORDER BY ")
		orderClauses := make([]string, len(internalOptions.OrderBy))
		for i, order := range internalOptions.OrderBy {
			orderClauses[i] = order
		}
		sb.WriteString(strings.Join(orderClauses, ", "))
	}
	
	// Add LIMIT and OFFSET clauses
	if internalOptions.Limit != nil {
		sb.WriteString(fmt.Sprintf(" LIMIT %d", *internalOptions.Limit))
	}
	
	if internalOptions.Offset != nil {
		sb.WriteString(fmt.Sprintf(" OFFSET %d", *internalOptions.Offset))
	}
	
	sqlQuery := sb.String()
	
	resultSet, err := c.executor.Execute(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert ResultSet to QueryResult
	columns := make([]types.ColumnInfo, len(resultSet.Columns))
	for i, col := range resultSet.Columns {
		columns[i] = types.ColumnInfo{
			Name: col,
			Type: types.ColumnTypeString, // Placeholder type
		}
	}
	
	return &types.QueryResult{
		Rows:    resultSet.Rows,
		Columns: columns,
		Count:   int64(resultSet.Count),
	}, nil
}

// Update updates records in the specified table
func (c *Client) Update(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}, where map[string]interface{}) (*types.QueryResult, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("UPDATE %s SET ", tableName))
	
	// Convert data map to SET clause
	setClauses := make([]string, 0, len(data))
	for column, value := range data {
		switch v := value.(type) {
		case string:
			setClauses = append(setClauses, fmt.Sprintf("%s = '%s'", column, v))
		case int, int32, int64:
			setClauses = append(setClauses, fmt.Sprintf("%s = %v", column, v))
		case float32, float64:
			setClauses = append(setClauses, fmt.Sprintf("%s = %v", column, v))
		case bool:
			if v {
				setClauses = append(setClauses, fmt.Sprintf("%s = true", column))
			} else {
				setClauses = append(setClauses, fmt.Sprintf("%s = false", column))
			}
		default:
			setClauses = append(setClauses, fmt.Sprintf("%s = '%v'", column, v))
		}
	}
	
	sb.WriteString(strings.Join(setClauses, ", "))
	
	// Convert where conditions to WHERE clause
	if where != nil && len(where) > 0 {
		sb.WriteString(" WHERE ")
		whereClauses := make([]string, 0, len(where))
		for column, value := range where {
			switch v := value.(type) {
			case string:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", column, v))
			case int, int32, int64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case float32, float64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case bool:
				if v {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = true", column))
				} else {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = false", column))
				}
			default:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", column, v))
			}
		}
		sb.WriteString(strings.Join(whereClauses, " AND "))
	}
	
	sqlQuery := sb.String()
	
	resultSet, err := c.executor.Execute(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert ResultSet to QueryResult
	columns := make([]types.ColumnInfo, len(resultSet.Columns))
	for i, col := range resultSet.Columns {
		columns[i] = types.ColumnInfo{
			Name: col,
			Type: types.ColumnTypeString, // Placeholder type
		}
	}
	
	return &types.QueryResult{
		Rows:    resultSet.Rows,
		Columns: columns,
		Count:   int64(resultSet.Count),
	}, nil
}

// Delete deletes records from the specified table
func (c *Client) Delete(ctx context.Context, tenantID int64, tableName string, where map[string]interface{}) (*types.QueryResult, error) {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("DELETE FROM %s", tableName))
	
	// Convert where conditions to WHERE clause
	if where != nil && len(where) > 0 {
		sb.WriteString(" WHERE ")
		whereClauses := make([]string, 0, len(where))
		for column, value := range where {
			switch v := value.(type) {
			case string:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%s'", column, v))
			case int, int32, int64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case float32, float64:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = %v", column, v))
			case bool:
				if v {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = true", column))
				} else {
					whereClauses = append(whereClauses, fmt.Sprintf("%s = false", column))
				}
			default:
				whereClauses = append(whereClauses, fmt.Sprintf("%s = '%v'", column, v))
			}
		}
		sb.WriteString(strings.Join(whereClauses, " AND "))
	}
	
	sqlQuery := sb.String()
	
	resultSet, err := c.executor.Execute(ctx, sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert ResultSet to QueryResult
	columns := make([]types.ColumnInfo, len(resultSet.Columns))
	for i, col := range resultSet.Columns {
		columns[i] = types.ColumnInfo{
			Name: col,
			Type: types.ColumnTypeString, // Placeholder type
		}
	}
	
	return &types.QueryResult{
		Rows:    resultSet.Rows,
		Columns: columns,
		Count:   int64(resultSet.Count),
	}, nil
}