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
	
	t.Run("QueryPgStatDatabase", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_stat_database", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)
		
		// Check column names
		expectedColumns := []string{
			"datid", "datname", "numbackends", "xact_commit", "xact_rollback",
			"blks_read", "blks_hit", "tup_returned", "tup_fetched", "tup_inserted",
			"tup_updated", "tup_deleted", "stats_reset",
		}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
		
		// Check that we have database statistics
		assert.Greater(t, len(result.Rows), 0)
		row := result.Rows[0]
		assert.Equal(t, "pglitedb", row[1]) // datname should be "pglitedb"
	})
	
	t.Run("QueryPgStatDatabaseWithFilter", func(t *testing.T) {
		filter := map[string]interface{}{"datname": "pglitedb"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_stat_database", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)
		
		// Check that we get the filtered result
		row := result.Rows[0]
		assert.Equal(t, "pglitedb", row[1])
	})
	
	t.Run("QueryPgStatDatabaseWithNonMatchingFilter", func(t *testing.T) {
		filter := map[string]interface{}{"datname": "nonexistent"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_stat_database", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		// Should return empty result for non-matching filter
		assert.Equal(t, 0, len(result.Rows))
	})
	
	t.Run("QueryPgStatBgWriter", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_stat_bgwriter", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names
		expectedColumns := []string{
			"checkpoints_timed", "checkpoints_req", "checkpoint_write_time",
			"checkpoint_sync_time", "buffers_checkpoint", "buffers_clean",
			"maxwritten_clean", "buffers_backend", "buffers_backend_fsync",
			"buffers_alloc", "stats_reset",
		}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
		
		// Check that we have bgwriter statistics
		assert.Greater(t, len(result.Rows), 0)
	})
	
	t.Run("QueryPgIndex", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_index", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names
		expectedColumns := []string{
			"indexrelid", "indrelid", "indnatts", "indisunique", "indisprimary",
			"indisclustered", "indisvalid", "indkey", "indcollation", "indclass",
			"indoption", "indexpred",
		}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
	})
	
	t.Run("QueryPgInherits", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_inherits", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		
		// Check column names
		expectedColumns := []string{"inhrelid", "inhparent", "inhseqno"}
		assert.Equal(t, len(expectedColumns), len(result.Columns))
		for i, col := range result.Columns {
			assert.Equal(t, expectedColumns[i], col.Name)
		}
		
		// Should return empty result since we don't have inheritance
		assert.Equal(t, 0, len(result.Rows))
	})
}