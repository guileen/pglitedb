package client

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/guileen/pglitedb/storage"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

// Client provides a unified interface for interacting with the database
// It can be used both for embedded access and for connecting to a remote server
type Client struct {
	executor *sql.Executor
	planner  interface{ Executor() *sql.Executor }
	engine   interface{ Close() error } // Store reference to engine for cleanup
	manager  catalog.Manager            // Store reference to catalog manager for direct operations
}

// NewClient creates a new embedded client
func NewClient(dbPath string) *Client {
	// Create a pebble KV store with high-performance configuration
	config := storage.HighPerformancePebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create pebble kv: %v", err))
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	
	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}
	
	// Create SQL parser and planner with catalog
	// Use hybrid parser for better performance with caching
	parser := sql.NewHybridPGParser()
	planner := sql.NewEnhancedPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	
	return &Client{
		executor: exec,
		planner:  planner,
		engine:   eng,
		manager:  mgr,
	}
}

// NewClientWithConfig creates a new embedded client with a specific configuration
func NewClientWithConfig(dbPath string, config *storage.PebbleConfig) *Client {
	// Create a pebble KV store with the provided configuration
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		panic(fmt.Sprintf("failed to create pebble kv: %v", err))
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	
	// Load existing schemas
	if err := mgr.LoadSchemas(context.Background()); err != nil {
		log.Printf("Warning: failed to load schemas: %v", err)
	}
	
	// Create SQL parser and planner with catalog
	// Use hybrid parser for better performance with caching
	parser := sql.NewHybridPGParser()
	planner := sql.NewEnhancedPlannerWithCatalog(parser, mgr)
	
	// Get executor from planner
	exec := planner.Executor()
	
	return &Client{
		executor: exec,
		planner:  planner,
		engine:   eng,
		manager:  mgr,
	}
}

// NewClientWithExecutor creates a new client with a custom executor
func NewClientWithExecutor(exec *sql.Executor, planner *sql.Planner) *Client {
	return &Client{
		executor: exec,
		planner:  planner,
	}
}

// Close closes the client and releases all resources
func (c *Client) Close() error {
	// Close the engine if it has a Close method
	if c.engine != nil {
		if err := c.engine.Close(); err != nil {
			return fmt.Errorf("failed to close engine: %w", err)
		}
	}
	
	return nil
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

// DirectInsert inserts a new record directly through the engine, bypassing SQL parsing
func (c *Client) DirectInsert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (int64, error) {
	if c.manager == nil {
		return 0, fmt.Errorf("direct operations not available")
	}
	
	return c.manager.InsertRow(ctx, tenantID, tableName, data)
}

// Insert inserts a new record into the specified table
func (c *Client) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.QueryResult, error) {
	// Validate data map to prevent empty column names
	for column := range data {
		if column == "" {
			return nil, fmt.Errorf("empty column name found in insert data")
		}
	}

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

// DirectBatchInsert inserts multiple records directly through the engine, bypassing SQL parsing
func (c *Client) DirectBatchInsert(ctx context.Context, tenantID int64, tableName string, dataList []map[string]interface{}) (int64, error) {
	if c.manager == nil {
		return 0, fmt.Errorf("direct operations not available")
	}
	
	// Use batch insert for better performance
	count, err := c.manager.InsertBatch(ctx, tenantID, tableName, dataList)
	if err != nil {
		return 0, err
	}
	
	return int64(len(count)), nil
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
	// Validate data map to prevent empty column names
	for column := range data {
		if column == "" {
			return nil, fmt.Errorf("empty column name found in update data")
		}
	}
	
	// Validate where conditions to prevent empty column names
	if where != nil {
		for column := range where {
			if column == "" {
				return nil, fmt.Errorf("empty column name found in where conditions")
			}
		}
	}

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

// statementTypeToString converts StatementType to string representation
func statementTypeToString(st parser.StatementType) string {
	switch st {
	case parser.SelectStatement:
		return "SELECT"
	case parser.InsertStatement:
		return "INSERT"
	case parser.UpdateStatement:
		return "UPDATE"
	case parser.DeleteStatement:
		return "DELETE"
	case parser.BeginStatement:
		return "BEGIN"
	case parser.CommitStatement:
		return "COMMIT"
	case parser.RollbackStatement:
		return "ROLLBACK"
	case parser.CreateTableStatement:
		return "CREATE_TABLE"
	case parser.DropTableStatement:
		return "DROP_TABLE"
	case parser.AlterTableStatement:
		return "ALTER_TABLE"
	case parser.CreateIndexStatement:
		return "CREATE_INDEX"
	case parser.DropIndexStatement:
		return "DROP_INDEX"
	case parser.CreateViewStatement:
		return "CREATE_VIEW"
	case parser.DropViewStatement:
		return "DROP_VIEW"
	case parser.AnalyzeStatementType:
		return "ANALYZE"
	default:
		return "UNKNOWN"
	}
}

// Explain explains a SQL query and returns the execution plan
func (c *Client) Explain(ctx context.Context, query interface{}) (*types.QueryResult, error) {
	// Convert query interface to string
	sqlQuery, ok := query.(string)
	if !ok {
		return nil, fmt.Errorf("invalid query type: expected string")
	}
	
	plan, err := c.executor.Explain(sqlQuery)
	if err != nil {
		return nil, err
	}
	
	// Convert plan to a readable string representation
	planStr := fmt.Sprintf("Plan Type: %s\nOperation: %s\nTable: %s\nFields: %v", 
		statementTypeToString(plan.Type), plan.Operation, plan.Table, plan.Fields)
	
	// Convert plan to QueryResult
	return &types.QueryResult{
		Rows: [][]interface{}{{planStr}},
		Columns: []types.ColumnInfo{
			{Name: "plan", Type: types.ColumnTypeString},
		},
		Count: 1,
	}, nil
}

// BatchInsert inserts multiple records into the specified table in a single batch operation
func (c *Client) BatchInsert(ctx context.Context, tenantID int64, tableName string, dataList []map[string]interface{}) (*types.QueryResult, error) {
	if len(dataList) == 0 {
		return &types.QueryResult{
			Count: 0,
		}, nil
	}
	
	// Validate data maps to prevent empty column names
	for _, data := range dataList {
		for column := range data {
			if column == "" {
				return nil, fmt.Errorf("empty column name found in batch insert data")
			}
		}
	}

	// Pre-allocate string builder with estimated capacity to reduce reallocations
	estimatedCapacity := 100 + len(tableName) + len(dataList)*50 // Rough estimate
	var sb strings.Builder
	sb.Grow(estimatedCapacity)
	
	sb.WriteString("INSERT INTO ")
	sb.WriteString(tableName)
	sb.WriteString(" (")
	
	// Get columns from the first record
	firstRecord := dataList[0]
	columns := make([]string, 0, len(firstRecord))
	for column := range firstRecord {
		columns = append(columns, column)
	}
	
	// Use strings.Join for columns to reduce allocations
	sb.WriteString(strings.Join(columns, ", "))
	sb.WriteString(") VALUES ")
	
	// Add values for each record with minimal allocations
	for i, data := range dataList {
		if i > 0 {
			sb.WriteString(", ")
		}
		
		sb.WriteString("(")
		for j, column := range columns {
			if j > 0 {
				sb.WriteString(", ")
			}
			
			value, exists := data[column]
			if !exists {
				sb.WriteString("NULL")
				continue
			}
			
			// Use type switches with direct string building to avoid fmt.Sprintf overhead
			switch v := value.(type) {
			case string:
				sb.WriteString("'")
				sb.WriteString(v)
				sb.WriteString("'")
			case int:
				sb.WriteString(strconv.FormatInt(int64(v), 10))
			case int32:
				sb.WriteString(strconv.FormatInt(int64(v), 10))
			case int64:
				sb.WriteString(strconv.FormatInt(v, 10))
			case float32:
				sb.WriteString(strconv.FormatFloat(float64(v), 'g', -1, 32))
			case float64:
				sb.WriteString(strconv.FormatFloat(v, 'g', -1, 64))
			case bool:
				if v {
					sb.WriteString("true")
				} else {
					sb.WriteString("false")
				}
			default:
				sb.WriteString("'")
				sb.WriteString(fmt.Sprintf("%v", v))
				sb.WriteString("'")
			}
		}
		sb.WriteString(")")
	}
	
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