package catalog

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/catalog/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestSchemaManager(t *testing.T) (SchemaManager, func()) {
	// Create an in-memory KV store for testing
	kv, err := storage.NewPebbleKV(storage.TestOptimizedPebbleConfig(""))
	require.NoError(t, err)

	// Create a mock ID generator
	idGen := &mockIDGenerator{tableID: 0, rowID: 0}

	// Create schema manager
	manager := newSchemaManager(idGen, kv, newTestSchemaCache())

	cleanup := func() {
		kv.Close()
	}

	return manager, cleanup
}

type mockIDGenerator struct {
	tableID int64
	rowID   int64
}

func (m *mockIDGenerator) NextTableID(ctx context.Context, tenantID int64) (int64, error) {
	m.tableID++
	return m.tableID, nil
}

func (m *mockIDGenerator) NextIndexID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	return 1, nil
}

func (m *mockIDGenerator) NextRowID(ctx context.Context, tenantID, tableID int64) (int64, error) {
	m.rowID++
	return m.rowID, nil
}

func newTestSchemaCache() *internal.SchemaCache {
	return internal.NewSchemaCache()
}

func intPtr(i int) *int {
	return &i
}

func TestSchemaManager_CreateTable(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Test creating a table
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

	// Verify table was created
	retrievedDef, err := manager.GetTableDefinition(ctx, 1, "users")
	require.NoError(t, err)
	assert.Equal(t, tableDef.Name, retrievedDef.Name)
	assert.Equal(t, len(tableDef.Columns), len(retrievedDef.Columns))
	assert.WithinDuration(t, time.Now(), retrievedDef.CreatedAt, time.Second)
	assert.WithinDuration(t, time.Now(), retrievedDef.UpdatedAt, time.Second)
	assert.Equal(t, 1, retrievedDef.Version)
}

func TestSchemaManager_CreateTable_AlreadyExists(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	// Create table first time
	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Try to create the same table again - should fail
	err = manager.CreateTable(ctx, 1, tableDef)
	assert.Error(t, err)
}

func TestSchemaManager_DropTable(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table first
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Drop the table
	err = manager.DropTable(ctx, 1, "users")
	require.NoError(t, err)

	// Verify table was dropped
	_, err = manager.GetTableDefinition(ctx, 1, "users")
	assert.Error(t, err)
	assert.True(t, errors.IsTableNotFoundError(err))
}

func TestSchemaManager_DropTable_NotFound(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Try to drop a non-existent table
	err := manager.DropTable(ctx, 1, "nonexistent")
	assert.Error(t, err)
	assert.True(t, errors.IsTableNotFoundError(err))
}

func TestSchemaManager_AlterTable_AddColumns(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Alter table to add columns
	changes := &AlterTableChanges{
		AddColumns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false, MaxLength: intPtr(100)},
			{Name: "email", Type: types.ColumnTypeVarchar, Nullable: true, MaxLength: intPtr(255)},
		},
	}

	err = manager.AlterTable(ctx, 1, "users", changes)
	require.NoError(t, err)

	// Verify changes
	retrievedDef, err := manager.GetTableDefinition(ctx, 1, "users")
	require.NoError(t, err)
	assert.Equal(t, 3, len(retrievedDef.Columns)) // id, name, email
	assert.Equal(t, 2, retrievedDef.Version) // Version should be incremented
}

func TestSchemaManager_AlterTable_DropColumns(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table with multiple columns
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

	// Alter table to drop a column
	changes := &AlterTableChanges{
		DropColumns: []string{"email"},
	}

	err = manager.AlterTable(ctx, 1, "users", changes)
	require.NoError(t, err)

	// Verify changes
	retrievedDef, err := manager.GetTableDefinition(ctx, 1, "users")
	require.NoError(t, err)
	assert.Equal(t, 2, len(retrievedDef.Columns)) // id, name
	assert.Equal(t, 2, retrievedDef.Version) // Version should be incremented
}

func TestSchemaManager_ListTables(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create multiple tables
	tables := []string{"users", "products", "orders"}
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

	// List tables
	listedTables, err := manager.ListTables(ctx, 1)
	require.NoError(t, err)
	assert.Equal(t, len(tables), len(listedTables))

	// Verify table names
	tableNames := make(map[string]bool)
	for _, table := range listedTables {
		tableNames[table.Name] = true
	}
	for _, tableName := range tables {
		assert.True(t, tableNames[tableName])
	}
}

func TestSchemaManager_ValidateConstraint_PrimaryKey(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false, MaxLength: intPtr(100)},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Validate primary key constraint
	constraint := &types.ConstraintDef{
		Name:    "pk_users",
		Type:    "primary_key",
		Columns: []string{"id"},
	}

	err = manager.ValidateConstraint(ctx, 1, "users", constraint)
	assert.NoError(t, err)
}

func TestSchemaManager_ValidateConstraint_Unique(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "email", Type: types.ColumnTypeVarchar, Nullable: false, MaxLength: intPtr(255)},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Validate unique constraint
	constraint := &types.ConstraintDef{
		Name:    "uk_users_email",
		Type:    "unique",
		Columns: []string{"email"},
	}

	err = manager.ValidateConstraint(ctx, 1, "users", constraint)
	assert.NoError(t, err)
}

func TestSchemaManager_ValidateConstraint_ColumnNotFound(t *testing.T) {
	manager, cleanup := setupTestSchemaManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
		},
	}

	err := manager.CreateTable(ctx, 1, tableDef)
	require.NoError(t, err)

	// Try to validate constraint with non-existent column
	constraint := &types.ConstraintDef{
		Name:    "uk_users_email",
		Type:    "unique",
		Columns: []string{"email"}, // This column doesn't exist
	}

	err = manager.ValidateConstraint(ctx, 1, "users", constraint)
	assert.Error(t, err)
}