package client

import (
	"context"
	"fmt"
	"strings"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/protocol/executor"
	"github.com/guileen/pglitedb/storage"
	
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
)

// Client provides a unified interface for interacting with the database
// It can be used both for embedded access and for connecting to a remote server
type Client struct {
	executor executor.QueryExecutor
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
	mgr := catalog.NewTableManager(eng)
	exec := executor.NewExecutor(mgr, eng)

	return &Client{
		executor: exec,
	}
}

// NewClientWithExecutor creates a new client with a custom executor
func NewClientWithExecutor(exec executor.QueryExecutor) *Client {
	return &Client{
		executor: exec,
	}
}

// convertInternalToExternalResult converts internal executor.QueryResult to external client.QueryResult
func convertInternalToExternalResult(internalResult *types.QueryResult) *types.QueryResult {
	if internalResult == nil {
		return &types.QueryResult{
			Rows:    []map[string]interface{}{},
			Count:   0,
			HasMore: false,
		}
	}

	// If Rows is already populated, return as is
	if len(internalResult.Rows) > 0 || internalResult.Records == nil {
		return internalResult
	}

	// Convert Records to Rows if needed
	records, ok := internalResult.Records.([]*types.Record)
	if !ok {
		return internalResult
	}

	rows := make([]map[string]interface{}, len(records))
	for i, record := range records {
		row := make(map[string]interface{})
		for key, value := range record.Data {
			if value != nil {
				row[key] = value.Data
			}
		}
		rows[i] = row
	}

	return &types.QueryResult{
		Rows:    rows,
		Count:   internalResult.Count,
		HasMore: internalResult.HasMore,
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
	// Convert query interface to executor.Query
	execQuery, ok := query.(*executor.Query)
	if !ok {
		return nil, fmt.Errorf("invalid query type: expected *executor.Query")
	}

	result, err := c.executor.Execute(ctx, execQuery)
	if err != nil {
		return nil, err
	}

	return convertInternalToExternalResult(result), nil
}

// Explain generates an execution plan for a query without executing it
func (c *Client) Explain(ctx context.Context, query interface{}) (interface{}, error) {
	// Convert query interface to executor.Query
	execQuery, ok := query.(*executor.Query)
	if !ok {
		return nil, fmt.Errorf("invalid query type: expected *executor.Query")
	}

	plan, err := c.executor.Explain(ctx, execQuery)
	if err != nil {
		return nil, err
	}

	return plan, nil
}

// Insert inserts a new record into the specified table
func (c *Client) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*types.QueryResult, error) {
	// Convert data map to executor.Value map
	values := make(map[string]*types.Value)
	for key, value := range data {
		values[key] = &types.Value{
			Data: value,
			Type: types.GetColumnTypeFromGoType(fmt.Sprintf("%T", value)),
		}
	}

	query := &executor.Query{
		Type:      executor.QueryTypeInsert,
		TableName: tableName,
		TenantID:  tenantID,
		Insert: &executor.InsertQuery{
			Values: values,
		},
	}

	result, err := c.executor.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	return convertInternalToExternalResult(result), nil
}

// Select retrieves records from the specified table
func (c *Client) Select(ctx context.Context, tenantID int64, tableName string, options *types.QueryOptions) (*types.QueryResult, error) {
	internalOptions := convertExternalToInternalOptions(options)

	// Convert where conditions to executor filters
	var filters []executor.Filter
	if internalOptions.Where != nil {
		for column, value := range internalOptions.Where {
			filters = append(filters, executor.Filter{
				Column:   column,
				Operator: executor.OpEqual,
				Value:    value,
			})
		}
	}

	// Convert order by options
	var orderBy []executor.OrderByClause
	if internalOptions.OrderBy != nil {
		for _, order := range internalOptions.OrderBy {
			// Parse order string like "name DESC" or "age ASC"
			desc := false
			column := order
			if len(order) > 5 && order[len(order)-4:] == "DESC" {
				desc = true
				column = order[:len(order)-5]
			} else if len(order) > 4 && order[len(order)-3:] == "ASC" {
				column = order[:len(order)-4]
			}
			orderBy = append(orderBy, executor.OrderByClause{
				Column:     strings.TrimSpace(column),
				Descending: desc,
			})
		}
	}

	query := &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: tableName,
		TenantID:  tenantID,
		Select: &executor.SelectQuery{
			Columns: internalOptions.Columns,
			Where:   filters,
			OrderBy: orderBy,
			Limit:   0,
			Offset:  0,
		},
	}

	// Apply limit and offset if provided
	if internalOptions.Limit != nil {
		query.Select.Limit = *internalOptions.Limit
	}
	if internalOptions.Offset != nil {
		query.Select.Offset = *internalOptions.Offset
	}

	result, err := c.executor.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	return convertInternalToExternalResult(result), nil
}

// Update updates records in the specified table
func (c *Client) Update(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}, where map[string]interface{}) (*types.QueryResult, error) {
	// Convert data map to executor.Value map
	values := make(map[string]*types.Value)
	for key, value := range data {
		values[key] = &types.Value{
			Data: value,
			Type: types.GetColumnTypeFromGoType(fmt.Sprintf("%T", value)),
		}
	}

	// Convert where conditions to executor filters
	var filters []executor.Filter
	for column, value := range where {
		filters = append(filters, executor.Filter{
			Column:   column,
			Operator: executor.OpEqual,
			Value:    value,
		})
	}

	query := &executor.Query{
		Type:      executor.QueryTypeUpdate,
		TableName: tableName,
		TenantID:  tenantID,
		Update: &executor.UpdateQuery{
			Values: values,
			Where:  filters,
		},
	}

	result, err := c.executor.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	return convertInternalToExternalResult(result), nil
}

// Delete deletes records from the specified table
func (c *Client) Delete(ctx context.Context, tenantID int64, tableName string, where map[string]interface{}) (*types.QueryResult, error) {
	// Convert where conditions to executor filters
	var filters []executor.Filter
	for column, value := range where {
		filters = append(filters, executor.Filter{
			Column:   column,
			Operator: executor.OpEqual,
			Value:    value,
		})
	}

	query := &executor.Query{
		Type:      executor.QueryTypeDelete,
		TableName: tableName,
		TenantID:  tenantID,
		Delete: &executor.DeleteQuery{
			Where: filters,
		},
	}

	result, err := c.executor.Execute(ctx, query)
	if err != nil {
		return nil, err
	}

	return convertInternalToExternalResult(result), nil
}