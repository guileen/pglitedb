package catalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestPgBenchRegression verifies that the pgbench regression test issue is fixed
// This test simulates the scenario where pgbench queries system tables to find table metadata
func TestPgBenchRegression(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create the pgbench_branches table that was causing the "table not found" error
	tableDef := &types.TableDefinition{
		Name: "pgbench_branches",
		Columns: []types.ColumnDefinition{
			{Name: "bid", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "bbalance", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "filler", Type: types.ColumnTypeText, Nullable: true},
		},
	}
	
	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Simulate pgbench querying pg_class for the table
	t.Run("QueryPgClassForPgBenchBranches", func(t *testing.T) {
		filter := map[string]interface{}{"relname": "pgbench_branches"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_class", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result.Rows), "Should find exactly one row for pgbench_branches table")
		
		// Verify that the row contains proper OID
		if len(result.Rows) > 0 {
			row := result.Rows[0]
			assert.NotZero(t, row[0], "OID should not be zero")
			assert.Equal(t, "pgbench_branches", row[1], "relname should match")
		}
	})

	// Simulate pgbench querying pg_attribute for the table columns
	t.Run("QueryPgAttributeForPgBenchBranches", func(t *testing.T) {
		filter := map[string]interface{}{"relname": "pgbench_branches"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_attribute", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0, "Should find attributes for pgbench_branches table")
		
		// Verify that attrelid is a proper OID (not a string)
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			attrelid := firstRow[0]
			assert.IsType(t, int64(0), attrelid, "attrelid should be int64 (OID)")
			assert.NotZero(t, attrelid, "attrelid should not be zero")
		}
	})

	// Simulate pgbench querying pg_namespace
	t.Run("QueryPgNamespace", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_namespace", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0, "Should find namespaces")
		
		// Verify that namespace rows contain proper OIDs
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			assert.NotZero(t, firstRow[0], "Namespace OID should not be zero")
		}
	})

	// Simulate pgbench querying pg_type
	t.Run("QueryPgType", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_type", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0, "Should find types")
		
		// Verify that type rows contain proper OIDs
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			assert.NotZero(t, firstRow[0], "Type OID should not be zero")
		}
	})
}