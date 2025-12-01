package sql

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/protocol/sql/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestExecutor_CreateTableIfNotExists verifies that CREATE TABLE IF NOT EXISTS statements are properly handled
func TestExecutor_CreateTableIfNotExists(t *testing.T) {
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

	// Test CREATE TABLE IF NOT EXISTS statement type recognition
	createTests := []struct {
		query    string
		expected parser.StatementType
	}{
		{"CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)", parser.CreateTableStatement},
		{"CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, name TEXT)", parser.CreateTableStatement},
		{"create table if not exists test_table (id integer primary key, name text)", parser.CreateTableStatement},
		{"Create Table If Not Exists test_table (id integer primary key, name text)", parser.CreateTableStatement},
	}

	for _, tt := range createTests {
		t.Run("Create_"+tt.query, func(t *testing.T) {
			// Test that the parser correctly identifies CREATE TABLE statements
			stmtType := planner.parser.getStatementType(tt.query)
			assert.Equal(t, tt.expected, stmtType, "Query: %s", tt.query)

			// Test that the executor can handle the statement without returning "unsupported statement type: UNKNOWN"
			ctx := context.Background()
			result, err := executor.Execute(ctx, tt.query)
			// The error should not be "unsupported statement type: UNKNOWN"
			if err != nil {
				assert.NotContains(t, err.Error(), "unsupported statement type: UNKNOWN", "Query: %s", tt.query)
			} else {
				// Verify that we get a result
				assert.NotNil(t, result)
			}
		})
	}

	// Test the actual behavior of CREATE TABLE IF NOT EXISTS
	ctx := context.Background()

	// First creation should succeed
	query1 := "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"
	result1, err1 := executor.Execute(ctx, query1)
	require.NoError(t, err1)
	assert.NotNil(t, result1)

	// Parse the query to check if IfNotExists is set correctly
	parsed1, err := sqlParser.Parse(query1)
	require.NoError(t, err)
	assert.True(t, parsed1.IfNotExists, "IfNotExists should be true for query: %s", query1)

	// Second creation should also succeed (IF NOT EXISTS should prevent error)
	query2 := "CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT)"
	result2, err2 := executor.Execute(ctx, query2)
	require.NoError(t, err2)
	assert.NotNil(t, result2)

	// Parse the query to check if IfNotExists is set correctly
	parsed2, err := sqlParser.Parse(query2)
	require.NoError(t, err)
	assert.True(t, parsed2.IfNotExists, "IfNotExists should be true for query: %s", query2)

	// Verify that a regular CREATE TABLE fails when table exists
	query3 := "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"
	result3, err3 := executor.Execute(ctx, query3)
	require.Error(t, err3)
	assert.Contains(t, err3.Error(), "failed to create table")
	assert.Contains(t, err3.Error(), "table already exists")
	assert.Nil(t, result3)

	// Parse the query to check if IfNotExists is set correctly
	parsed3, err := sqlParser.Parse(query3)
	require.NoError(t, err)
	assert.False(t, parsed3.IfNotExists, "IfNotExists should be false for query: %s", query3)

	// Test with complex table definition
	query4 := `CREATE TABLE IF NOT EXISTS complex_users (
		id SERIAL PRIMARY KEY,
		name VARCHAR(255) NOT NULL DEFAULT 'unknown',
		email VARCHAR(255) UNIQUE,
		age INTEGER CHECK (age >= 0),
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	)`

	result4, err4 := executor.Execute(ctx, query4)
	require.NoError(t, err4)
	assert.NotNil(t, result4)

	// Execute again - should not fail
	result5, err5 := executor.Execute(ctx, query4)
	require.NoError(t, err5)
	assert.NotNil(t, result5)
}

// TestExecutor_PgBenchCreateTableIfNotExists simulates the exact pgbench behavior
func TestExecutor_PgBenchCreateTableIfNotExists(t *testing.T) {
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

	// Simulate pgbench initial table creation with exact pgbench queries
	queries := []string{
		"CREATE TABLE IF NOT EXISTS pgbench_branches (bid integer NOT NULL, bbalance integer, filler character(88), PRIMARY KEY (bid))",
		"CREATE TABLE IF NOT EXISTS pgbench_tellers (tid integer NOT NULL, bid integer, tbalance integer, filler character(84), PRIMARY KEY (tid))",
		"CREATE TABLE IF NOT EXISTS pgbench_accounts (aid integer NOT NULL, bid integer, abalance integer, filler character(84), PRIMARY KEY (aid))",
		"CREATE TABLE IF NOT EXISTS pgbench_history (tid integer, bid integer, aid integer, delta integer, mtime timestamp, filler character(22))",
	}

	// First run - all should succeed
	for i, query := range queries {
		t.Run(fmt.Sprintf("PgBench_FirstRun_Query%d", i+1), func(t *testing.T) {
			result, err := executor.Execute(ctx, query)
			require.NoError(t, err)
			assert.NotNil(t, result)
		})
	}

	// Second run - all should still succeed because of IF NOT EXISTS
	for i, query := range queries {
		t.Run(fmt.Sprintf("PgBench_SecondRun_Query%d", i+1), func(t *testing.T) {
			result, err := executor.Execute(ctx, query)
			require.NoError(t, err)
			assert.NotNil(t, result)
		})
	}

	// Verify that regular CREATE TABLE fails on existing tables
	for i, query := range queries {
		t.Run(fmt.Sprintf("PgBench_RegularCreate_Query%d", i+1), func(t *testing.T) {
			// Remove IF NOT EXISTS from the query
			regularQuery := strings.Replace(query, "IF NOT EXISTS ", "", 1)
			result, err := executor.Execute(ctx, regularQuery)
			require.Error(t, err)
			assert.Contains(t, err.Error(), "failed to create table")
			assert.Contains(t, err.Error(), "table already exists")
			assert.Nil(t, result)
		})
	}
}