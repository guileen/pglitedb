package catalog

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestViewManager_CreateAndView(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a base table first
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false, MaxLength: intPtr(100)},
			{Name: "email", Type: types.ColumnTypeVarchar, Nullable: true, MaxLength: intPtr(255)},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Create a view
	err = manager.CreateView(ctx, 1, "user_emails", "SELECT id, email FROM users WHERE email IS NOT NULL", false)
	require.NoError(t, err)

	// Retrieve the view
	retrievedView, err := manager.GetViewDefinition(ctx, 1, "user_emails")
	require.NoError(t, err)
	assert.Equal(t, "user_emails", retrievedView.Name)
	assert.Equal(t, "SELECT id, email FROM users WHERE email IS NOT NULL", retrievedView.Query)
}

func TestViewManager_CreateView_AlreadyExists(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a base table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Create a view
	err = manager.CreateView(ctx, 1, "user_view", "SELECT id FROM users", false)
	require.NoError(t, err)

	// Try to create the same view again - should fail
	err = manager.CreateView(ctx, 1, "user_view", "SELECT id FROM users", false)
	assert.Error(t, err)
}

func TestViewManager_DropView(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a base table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Create a view
	err = manager.CreateView(ctx, 1, "user_view", "SELECT id FROM users", false)
	require.NoError(t, err)

	// Drop the view
	err = manager.DropView(ctx, 1, "user_view")
	require.NoError(t, err)

	// Verify view was dropped
	_, err = manager.GetViewDefinition(ctx, 1, "user_view")
	assert.Error(t, err)
}

func TestViewManager_DropView_NotFound(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to drop a non-existent view
	err := manager.DropView(ctx, 1, "nonexistent_view")
	assert.Error(t, err)
}

func TestViewManager_ListViews(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a base table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Create multiple views
	views := []string{"view1", "view2", "view3"}
	for _, viewName := range views {
		err := manager.CreateView(ctx, 1, viewName, "SELECT id FROM users", false)
		require.NoError(t, err)
	}

	// List views
	listedViews, err := manager.ListViews(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, len(views), len(listedViews))

	// Verify view names
	viewNames := make(map[string]bool)
	for _, view := range listedViews {
		viewNames[view.Name] = true
	}
	for _, viewName := range views {
		assert.True(t, viewNames[viewName])
	}
}

func TestViewManager_ViewDependencies(t *testing.T) {
	manager, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create base tables
	tables := []string{"users", "orders"}
	for _, tableName := range tables {
		tableDef := &types.TableDefinition{
			Name: tableName,
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			},
		}
		err := manager.CreateTable(ctx, 1, tableDef)
		require.NoError(t, err)
	}

	// Create a view that depends on multiple tables
	err := manager.CreateView(ctx, 1, "user_orders", "SELECT u.id as user_id, o.id as order_id FROM users u JOIN orders o ON u.id = o.user_id", false)
	require.NoError(t, err)

	// Retrieve the view and verify it exists
	retrievedView, err := manager.GetViewDefinition(ctx, 1, "user_orders")
	require.NoError(t, err)
	assert.Equal(t, "user_orders", retrievedView.Name)
}