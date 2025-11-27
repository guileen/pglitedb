package catalog

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSystemTablesFix(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test table
	tableDef := createUsersTable()
	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	t.Run("QueryPgClassWithOID", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_class", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)

		// Check that we have the oid column
		foundOID := false
		for _, col := range result.Columns {
			if col.Name == "oid" {
				foundOID = true
				break
			}
		}
		assert.True(t, foundOID, "pg_class should have oid column")

		// Check that rows have OID values
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			assert.NotZero(t, firstRow[0], "First column (oid) should not be zero")
		}
	})

	t.Run("QueryPgAttributeWithCorrectAttrelid", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_attribute", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)

		// Check that attrelid is a numeric OID, not a string
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			attrelid := firstRow[0]
			assert.IsType(t, int64(0), attrelid, "attrelid should be int64 (OID)")
		}
	})

	t.Run("QueryPgNamespaceWithOID", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_namespace", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)

		// Check that we have the oid column as first column
		if len(result.Columns) > 0 {
			assert.Equal(t, "oid", result.Columns[0].Name, "First column should be oid")
		}

		// Check that rows have OID values
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			assert.NotZero(t, firstRow[0], "First column (oid) should not be zero")
		}
	})

	t.Run("QueryPgTypeWithOID", func(t *testing.T) {
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_type", nil)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0)

		// Check that we have the oid column as first column
		if len(result.Columns) > 0 {
			assert.Equal(t, "oid", result.Columns[0].Name, "First column should be oid")
		}

		// Check that rows have OID values
		if len(result.Rows) > 0 {
			firstRow := result.Rows[0]
			assert.NotZero(t, firstRow[0], "First column (oid) should not be zero")
		}
	})

	t.Run("QuerySpecificTableMetadata", func(t *testing.T) {
		// Query pg_class for our specific table
		filter := map[string]interface{}{"relname": "users"}
		result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_class", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, 1, len(result.Rows), "Should find exactly one row for users table")

		// Query pg_attribute for our specific table
		result, err = manager.QuerySystemTable(ctx, "pg_catalog.pg_attribute", filter)
		require.NoError(t, err)
		assert.NotNil(t, result)
		assert.Greater(t, len(result.Rows), 0, "Should find attributes for users table")
	})
}