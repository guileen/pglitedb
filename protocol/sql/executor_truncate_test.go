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

// TestExecutor_Truncate tests the TRUNCATE TABLE functionality
func TestExecutor_Truncate(t *testing.T) {
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

	// Test TRUNCATE TABLE statement on empty table
	truncateQuery := "TRUNCATE TABLE test_table"
	result, err = executor.Execute(ctx, truncateQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("TRUNCATE result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// According to PostgreSQL docs, TRUNCATE should return Count = 0 (no affected row count)
	assert.Equal(t, 0, result.Count, "TRUNCATE should return Count = 0, but got %d", result.Count)
	
	// Also verify that the result structure is as expected
	assert.Empty(t, result.Columns, "TRUNCATE result should have no columns")
	assert.Empty(t, result.Rows, "TRUNCATE result should have no rows")

	// Test TRUNCATE on a table with data
	// First insert some data
	insertQuery := "INSERT INTO test_table (id, name) VALUES (1, 'test')"
	result, err = executor.Execute(ctx, insertQuery)
	require.NoError(t, err)

	// Now truncate again
	result, err = executor.Execute(ctx, truncateQuery)
	require.NoError(t, err)
	
	// Log what we get
	t.Logf("TRUNCATE with data result: Count=%d, Columns=%v, Rows=%v", result.Count, result.Columns, result.Rows)
	
	// According to PostgreSQL docs, TRUNCATE should always return Count = 0
	assert.Equal(t, 0, result.Count, "TRUNCATE should return Count = 0 even when deleting rows")
	
	// Structure should still be empty
	assert.Empty(t, result.Columns, "TRUNCATE result should have no columns")
	assert.Empty(t, result.Rows, "TRUNCATE result should have no rows")
}