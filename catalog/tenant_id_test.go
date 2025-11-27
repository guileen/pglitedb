package catalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDifferentTenantID(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create the pgbench_branches table with tenant ID 2 (different from hardcoded 1)
	tableDef := &types.TableDefinition{
		Name: "pgbench_branches",
		Columns: []types.ColumnDefinition{
			{Name: "bid", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "bbalance", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "filler", Type: types.ColumnTypeText, Nullable: true},
		},
	}
	
	err := manager.CreateTable(ctx, 2, tableDef) // Using tenant ID 2
	require.NoError(t, err)

	// Query pg_class for the table - this should fail because queryPgClass hardcodes tenant ID 1
	filter := map[string]interface{}{"relname": "pgbench_branches"}
	result, err := manager.QuerySystemTable(ctx, "pg_catalog.pg_class", filter)
	require.NoError(t, err)
	assert.Equal(t, 0, len(result.Rows), "Should not find the table with different tenant ID")
}