package catalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog/errors"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSchemaManager_CreateTableIfNotExists(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeText, Nullable: false},
		},
	}

	// First creation should succeed
	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Second creation without IF NOT EXISTS should fail
	err = manager.CreateTable(ctx, 1, tableDef)
	require.Error(t, err)
	assert.True(t, errors.IsTableAlreadyExistsError(err))

	// Using CreateTableIfNotExists should succeed even if table exists
	err = manager.CreateTableIfNotExists(ctx, 1, tableDef)
	require.NoError(t, err)

	// Test with a new table - should also succeed
	newTableDef := &types.TableDefinition{
		Name: "new_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err = manager.CreateTableIfNotExists(ctx, 1, newTableDef)
	require.NoError(t, err)

	// Creating the same new table again should also succeed
	err = manager.CreateTableIfNotExists(ctx, 1, newTableDef)
	require.NoError(t, err)
}