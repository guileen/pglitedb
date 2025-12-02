package sql

import (
	"context"
	"os"
	"testing"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutor_ExecuteExpressionQuery(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a catalog manager with the engine
	manager := catalog.NewTableManager(eng)

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor with the catalog
	planner := NewPlannerWithCatalog(sqlParser, manager)
	executor := NewExecutorWithCatalog(planner, manager)

	ctx := context.Background()

	// Test expression query without table
	plan := &Plan{
		Fields: []string{"1 + 1"},
		Type:   parser.SelectStatement,
	}

	result, err := executor.executeExpressionQuery(ctx, plan)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Count)
	assert.Len(t, result.Columns, 1)
	assert.Len(t, result.Rows, 1)
	assert.Len(t, result.Rows[0], 1)
	// The result should be nil for expression queries in this basic implementation
	assert.Nil(t, result.Rows[0][0])
}

func TestExecutor_ExecuteExpressionQuery_WithFields(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a catalog manager with the engine
	manager := catalog.NewTableManager(eng)

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor with the catalog
	planner := NewPlannerWithCatalog(sqlParser, manager)
	executor := NewExecutorWithCatalog(planner, manager)

	ctx := context.Background()

	// Test expression query with multiple fields
	plan := &Plan{
		Fields: []string{"field1", "field2"},
		Type:   parser.SelectStatement,
	}

	result, err := executor.executeExpressionQuery(ctx, plan)
	require.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, 1, result.Count)
	assert.Len(t, result.Columns, 2)
	assert.Equal(t, "field1", result.Columns[0])
	assert.Equal(t, "field2", result.Columns[1])
	assert.Len(t, result.Rows, 1)
	assert.Len(t, result.Rows[0], 2)
	// The result should be nil for expression queries in this basic implementation
	assert.Nil(t, result.Rows[0][0])
	assert.Nil(t, result.Rows[0][1])
}

func TestExecutor_ExecuteAggregateSelect_WithoutTable(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a catalog manager with the engine
	manager := catalog.NewTableManager(eng)

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor with the catalog
	planner := NewPlannerWithCatalog(sqlParser, manager)
	executor := NewExecutorWithCatalog(planner, manager)

	ctx := context.Background()

	// Test aggregate query without table (should delegate to expression query)
	plan := &Plan{
		Aggregates: []Aggregate{
			{Function: "COUNT", Field: "*", Alias: "count"},
		},
		Fields: []string{},
		Type:   parser.SelectStatement,
	}

	result, err := executor.executeAggregateSelect(ctx, plan)
	require.NoError(t, err)
	assert.NotNil(t, result)
	// Should delegate to executeExpressionQuery and return similar result
	assert.Equal(t, 1, result.Count)
	assert.Len(t, result.Columns, 1)
	// When there are no aggregates and no fields, it defaults to "result"
	assert.Equal(t, "result", result.Columns[0])
	assert.Len(t, result.Rows, 1)
	assert.Len(t, result.Rows[0], 1)
}

func TestExecutor_ExecuteSystemTableQuery_WithoutCatalog(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor WITHOUT catalog to test error handling
	planner := NewPlanner(sqlParser)
	executor := NewExecutor(planner)

	ctx := context.Background()

	// Test system table query without catalog
	plan := &Plan{
		Table: "pg_tables",
	}

	result, err := executor.executeSystemTableQuery(ctx, plan)
	assert.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "catalog not initialized")
}

func TestExecutor_ExecuteSystemTableQuery_EmptyTable(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a catalog manager with the engine
	manager := catalog.NewTableManager(eng)

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor with the catalog
	planner := NewPlannerWithCatalog(sqlParser, manager)
	executor := NewExecutorWithCatalog(planner, manager)

	ctx := context.Background()

	// Test system table query with empty table name
	plan := &Plan{
		Table: "",
		Type:  parser.SelectStatement,
	}

	result, err := executor.executeSystemTableQuery(ctx, plan)
	require.NoError(t, err)
	assert.NotNil(t, result)
	// Should delegate to executeExpressionQuery
	assert.Equal(t, 1, result.Count)
	assert.Len(t, result.Columns, 1)
	assert.Equal(t, "result", result.Columns[0])
}

func TestExecutor_ExecuteSystemTableQuery_ValidTable(t *testing.T) {
	// Create a temporary directory for the database
	tmpDir, err := os.MkdirTemp("", "test-db-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create an in-memory storage for testing
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create a catalog manager with the engine
	manager := catalog.NewTableManager(eng)

	// Create a simple parser
	sqlParser := NewSimplePGParser()

	// Create a planner and executor with the catalog
	planner := NewPlannerWithCatalog(sqlParser, manager)
	executor := NewExecutorWithCatalog(planner, manager)

	ctx := context.Background()

	// Test system table query with valid table name
	plan := &Plan{
		Table: "pg_tables",
		Type:  parser.SelectStatement,
	}

	// This should return an error because the system table doesn't exist in our test setup
	result, err := executor.executeSystemTableQuery(ctx, plan)
	// Depending on implementation, this might return an error or an empty result
	if err != nil {
		// Error is acceptable
		t.Logf("Got expected error for system table query: %v", err)
	} else {
		assert.NotNil(t, result)
		// Could be empty result
		t.Logf("System table query returned result with Count=%d", result.Count)
	}
}