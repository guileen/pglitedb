package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemTables(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()
	
	ctx := context.Background()
	
	// Create a test table
	tableDef := createUsersTable()
	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)
	
	t.Run("QueryPgTables", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_tables", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)
		
		// Check column names
		expectedColumns := []string{"tablename", "schemaname", "tableowner", "hasindexes", "hasrules", "hastriggers", "rowsecurity"}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
		
		// Check that our test table is in the results
		found := false
		for _, row := range result.Rows {
			if row[0] == "users" {
				found = true
				break
			}
		}
		assert.True(t, found, "users table should be in pg_tables")
	})
	
	t.Run("QueryPgColumns", func(t *testing.T) {
		filter := map[string]interface{}{"tablename": "users"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_columns", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names
		expectedColumns := []string{"tablename", "columnname", "schemaname", "datatype", "ordinal_position", "notnull", "column_default", "is_primary_key", "is_unique", "is_serial"}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
		
		// Check that we have columns for our test table
		assert.Greater(t, len(result.Rows), 0)
	})
	
	t.Run("QueryInformationSchemaTables", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "information_schema.tables", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)
	})
	
	t.Run("QueryInformationSchemaColumns", func(t *testing.T) {
		filter := map[string]interface{}{"table_name": "users"}
		result, err := manager.QuerySystemTable(ctx, "information_schema.columns", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)
	})
}