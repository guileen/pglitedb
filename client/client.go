package client

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/executor"
	"github.com/guileen/pglitedb/kv"
	"github.com/guileen/pglitedb/manager"
	"github.com/guileen/pglitedb/table"
)

// Client provides a unified interface for interacting with the database
// It can be used both for embedded access and for connecting to a remote server
type Client struct {
	executor executor.QueryExecutor
}

// NewClient creates a new embedded client
func NewClient(dbPath string) *Client {
	// Create a pebble KV store
	kvStore, err := kv.NewPebbleKV(kv.DefaultPebbleConfig(dbPath))
	if err != nil {
		panic(fmt.Sprintf("failed to create pebble kv: %v", err))
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine and manager
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := manager.NewTableManager(eng)
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

// Query executes a query and returns the result
func (c *Client) Query(ctx context.Context, query *executor.Query) (*executor.QueryResult, error) {
	return c.executor.Execute(ctx, query)
}

// Explain generates an execution plan for a query without executing it
func (c *Client) Explain(ctx context.Context, query *executor.Query) (*executor.QueryPlan, error) {
	return c.executor.Explain(ctx, query)
}

// Insert inserts a new record into the specified table
func (c *Client) Insert(ctx context.Context, tenantID int64, tableName string, data map[string]interface{}) (*executor.QueryResult, error) {
	values := make(map[string]*table.Value)
	for k, v := range data {
		values[k] = &table.Value{Data: v}
	}

	query := &executor.Query{
		Type:      executor.QueryTypeInsert,
		TableName: tableName,
		TenantID:  tenantID,
		Insert: &executor.InsertQuery{
			Values: values,
		},
	}

	return c.executor.Execute(ctx, query)
}

// Select retrieves records from the specified table
func (c *Client) Select(ctx context.Context, tenantID int64, tableName string, options *table.QueryOptions) (*executor.QueryResult, error) {
	query := &executor.Query{
		Type:      executor.QueryTypeSelect,
		TableName: tableName,
		TenantID:  tenantID,
		Select: &executor.SelectQuery{
			Columns: options.Columns,
			Where:   convertFilters(options.Where),
			Limit:   intPtrToInt(options.Limit),
			Offset:  intPtrToInt(options.Offset),
		},
	}

	return c.executor.Execute(ctx, query)
}

// Update updates records in the specified table
func (c *Client) Update(ctx context.Context, tenantID int64, tableName string, where map[string]interface{}, data map[string]interface{}) (*executor.QueryResult, error) {
	whereFilters := convertFilters(where)

	values := make(map[string]*table.Value)
	for k, v := range data {
		values[k] = &table.Value{Data: v}
	}

	query := &executor.Query{
		Type:      executor.QueryTypeUpdate,
		TableName: tableName,
		TenantID:  tenantID,
		Update: &executor.UpdateQuery{
			Where:  whereFilters,
			Values: values,
		},
	}

	return c.executor.Execute(ctx, query)
}

// Delete deletes records from the specified table
func (c *Client) Delete(ctx context.Context, tenantID int64, tableName string, where map[string]interface{}) (*executor.QueryResult, error) {
	whereFilters := convertFilters(where)

	query := &executor.Query{
		Type:      executor.QueryTypeDelete,
		TableName: tableName,
		TenantID:  tenantID,
		Delete: &executor.DeleteQuery{
			Where: whereFilters,
		},
	}

	return c.executor.Execute(ctx, query)
}

func convertFilters(filters map[string]interface{}) []executor.Filter {
	if filters == nil {
		return nil
	}

	result := make([]executor.Filter, 0, len(filters))
	for field, value := range filters {
		result = append(result, executor.Filter{
			Column:   field,
			Operator: executor.OpEqual,
			Value:    value,
		})
	}
	return result
}

func intPtrToInt(ptr *int) int {
	if ptr == nil {
		return 0
	}
	return *ptr
}
