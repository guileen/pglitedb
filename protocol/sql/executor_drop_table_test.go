package sql

import (
	"context"
	"os"
	"testing"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecutor_DropTableOperation tests the DROP TABLE functionality
func TestExecutor_DropTableOperation(t *testing.T) {
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

	// First, create a table
	createQuery := "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
	result, err := executor.Execute(ctx, createQuery)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Test DROP TABLE statement
	dropQuery := "DROP TABLE test_table"
	result, err = executor.Execute(ctx, dropQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("DROP TABLE result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// DROP TABLE should return an empty result set
	assert.Empty(t, result.Columns, "DROP TABLE result should have no columns")
	assert.Empty(t, result.Rows, "DROP TABLE result should have no rows")
	assert.Equal(t, 0, result.Count, "DROP TABLE should return Count = 0")

	// Test DROP TABLE on non-existent table (should fail)
	result, err = executor.Execute(ctx, dropQuery)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to drop table")
	assert.Contains(t, err.Error(), "not found")
	assert.Nil(t, result)
}

// TestExecutor_DropTableIfExists tests the DROP TABLE IF EXISTS functionality
func TestExecutor_DropTableIfExists(t *testing.T) {
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

	// Test DROP TABLE IF EXISTS on non-existent table (should succeed)
	dropQuery := "DROP TABLE IF EXISTS non_existent_table"
	result, err := executor.Execute(ctx, dropQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("DROP TABLE IF EXISTS result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// DROP TABLE IF EXISTS should return an empty result set
	assert.Empty(t, result.Columns, "DROP TABLE IF EXISTS result should have no columns")
	assert.Empty(t, result.Rows, "DROP TABLE IF EXISTS result should have no rows")
	assert.Equal(t, 0, result.Count, "DROP TABLE IF EXISTS should return Count = 0")

	// First, create a table
	createQuery := "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
	result, err = executor.Execute(ctx, createQuery)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Test DROP TABLE IF EXISTS on existing table (should succeed)
	dropQuery = "DROP TABLE IF EXISTS test_table"
	result, err = executor.Execute(ctx, dropQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("DROP TABLE IF EXISTS on existing table result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// DROP TABLE IF EXISTS should return an empty result set
	assert.Empty(t, result.Columns, "DROP TABLE IF EXISTS result should have no columns")
	assert.Empty(t, result.Rows, "DROP TABLE IF EXISTS result should have no rows")
	assert.Equal(t, 0, result.Count, "DROP TABLE IF EXISTS should return Count = 0")
}

// TestExecutor_DropTableWithCascade tests the DROP TABLE CASCADE functionality
func TestExecutor_DropTableWithCascade(t *testing.T) {
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

	// First, create a table
	createQuery := "CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"
	result, err := executor.Execute(ctx, createQuery)
	require.NoError(t, err)
	assert.NotNil(t, result)

	// Test DROP TABLE CASCADE statement
	dropQuery := "DROP TABLE test_table CASCADE"
	result, err = executor.Execute(ctx, dropQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("DROP TABLE CASCADE result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// DROP TABLE CASCADE should return an empty result set
	assert.Empty(t, result.Columns, "DROP TABLE CASCADE result should have no columns")
	assert.Empty(t, result.Rows, "DROP TABLE CASCADE result should have no rows")
	assert.Equal(t, 0, result.Count, "DROP TABLE CASCADE should return Count = 0")
}