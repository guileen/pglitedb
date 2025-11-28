package catalog

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestManager(t *testing.T) (Manager, func()) {
	tmpDir, err := os.MkdirTemp("", "manager-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create kv store: %v", err)
	}

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := NewTableManager(eng)

	cleanup := func() {
		eng.Close()
		os.RemoveAll(tmpDir)
	}

	return mgr, cleanup
}

func createUsersTable() *types.TableDefinition {
	return &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "email", Type: types.ColumnTypeString, Nullable: false, Unique: true},
			{Name: "age", Type: types.ColumnTypeNumber, Nullable: true},
			{Name: "active", Type: types.ColumnTypeBoolean, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_name", Columns: []string{"name"}},
		},
	}
}

func TestTableManager_CreateAndGetTable(t *testing.T) {
	mgr, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	tableDef := createUsersTable()

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		t.Fatalf("create table: %v", err)
	}

	retrieved, err := mgr.GetTableDefinition(ctx, 1, "users")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}

	if retrieved.Name != "users" {
		t.Errorf("expected name users, got %s", retrieved.Name)
	}

	if len(retrieved.Columns) != 4 {
		t.Errorf("expected 4 columns, got %d", len(retrieved.Columns))
	}
}

func TestTableManager_InsertAndGet(t *testing.T) {
	mgr, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	tableDef := createUsersTable()

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		t.Fatalf("create table: %v", err)
	}

	data := map[string]interface{}{
		"name":   "Alice",
		"email":  "alice@example.com",
		"age":    int64(30),
		"active": true,
	}

	record, err := mgr.Insert(ctx, 1, "users", data)
	if err != nil {
		t.Fatalf("insert: %v", err)
	}

	if record.ID == "" {
		t.Error("expected non-empty record ID")
	}

	if record.Data["name"].Data.(string) != "Alice" {
		t.Errorf("expected name Alice, got %v", record.Data["name"].Data)
	}
}

func TestTableManager_Query(t *testing.T) {
	mgr, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()
	tableDef := createUsersTable()

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		t.Fatalf("create table: %v", err)
	}

	users := []map[string]interface{}{
		{"name": "Alice", "email": "alice@example.com", "age": int64(30), "active": true},
		{"name": "Bob", "email": "bob@example.com", "age": int64(25), "active": true},
		{"name": "Charlie", "email": "charlie@example.com", "age": int64(35), "active": false},
		{"name": "David", "email": "david@example.com", "age": int64(28), "active": true},
		{"name": "Eve", "email": "eve@example.com", "age": int64(32), "active": false},
	}

	for _, user := range users {
		if _, err := mgr.Insert(ctx, 1, "users", user); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	t.Run("Query all", func(t *testing.T) {
		result, err := mgr.Query(ctx, 1, "users", nil)
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if result.Count != 5 {
			t.Errorf("expected 5 records, got %d", result.Count)
		}
	})

	t.Run("Query with filter", func(t *testing.T) {
		opts := &types.QueryOptions{
			Where: map[string]interface{}{
				"active": true,
			},
		}

		result, err := mgr.Query(ctx, 1, "users", opts)
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if result.Count != 3 {
			t.Errorf("expected 3 active records, got %d", result.Count)
		}
	})

	t.Run("Query with limit", func(t *testing.T) {
		limit := 2
		opts := &types.QueryOptions{
			Limit: &limit,
		}

		result, err := mgr.Query(ctx, 1, "users", opts)
		if err != nil {
			t.Fatalf("query: %v", err)
		}

		if result.Count != 2 {
			t.Errorf("expected 2 records, got %d", result.Count)
		}

		if !result.HasMore {
			t.Error("expected HasMore to be true")
		}
	})
}

func BenchmarkTableManager_Insert(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "manager-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := NewTableManager(eng)

	ctx := context.Background()
	tableDef := createUsersTable()

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		b.Fatalf("create table: %v", err)
	}

	data := map[string]interface{}{
		"name":   "BenchUser",
		"email":  "bench@example.com",
		"age":    int64(30),
		"active": true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data["email"] = "bench" + string(rune(i)) + "@example.com"
		if _, err := mgr.Insert(ctx, 1, "users", data); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}
}

func BenchmarkTableManager_Query(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "manager-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	mgr := NewTableManager(eng)

	ctx := context.Background()
	tableDef := createUsersTable()

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		b.Fatalf("create table: %v", err)
	}

	for i := 0; i < 100; i++ {
		data := map[string]interface{}{
			"name":   "User" + string(rune(i)),
			"email":  "user" + string(rune(i)) + "@example.com",
			"age":    int64(20 + i),
			"active": i%2 == 0,
		}
		if _, err := mgr.Insert(ctx, 1, "users", data); err != nil {
			b.Fatalf("insert: %v", err)
		}
	}

	limit := 10
	opts := &types.QueryOptions{Limit: &limit}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := mgr.Query(ctx, 1, "users", opts); err != nil {
			b.Fatalf("query: %v", err)
		}
	}
}

func TestTableManager_AdvancedDataTypes(t *testing.T) {
	mgr, cleanup := setupTestManager(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table with advanced data types
	tableDef := &types.TableDefinition{
		Name: "products",
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "description", Type: types.ColumnTypeText, Nullable: true},
			{Name: "price", Type: types.ColumnTypeNumber, Nullable: false},
			{Name: "in_stock", Type: types.ColumnTypeBoolean, Nullable: false},
			{Name: "metadata", Type: types.ColumnTypeJSON, Nullable: true},
			{Name: "product_id", Type: types.ColumnTypeUUID, Nullable: false},
			{Name: "image", Type: types.ColumnTypeBinary, Nullable: true},
		},
	}

	if err := mgr.CreateTable(ctx, 1, tableDef); err != nil {
		t.Fatalf("create table with advanced types: %v", err)
	}

	retrieved, err := mgr.GetTableDefinition(ctx, 1, "products")
	if err != nil {
		t.Fatalf("get table: %v", err)
	}

	if retrieved.Name != "products" {
		t.Errorf("expected name products, got %s", retrieved.Name)
	}

	if len(retrieved.Columns) != 7 {
		t.Errorf("expected 7 columns, got %d", len(retrieved.Columns))
	}

	// Verify column types
	columnTypes := make(map[string]types.ColumnType)
	for _, col := range retrieved.Columns {
		columnTypes[col.Name] = col.Type
	}

	if columnTypes["metadata"] != types.ColumnTypeJSON {
		t.Errorf("expected metadata to be JSON type, got %s", columnTypes["metadata"])
	}

	if columnTypes["product_id"] != types.ColumnTypeUUID {
		t.Errorf("expected product_id to be UUID type, got %s", columnTypes["product_id"])
	}

	if columnTypes["image"] != types.ColumnTypeBinary {
		t.Errorf("expected image to be Binary type, got %s", columnTypes["image"])
	}

	// Test inserting data with advanced types
	jsonData := map[string]interface{}{
		"color": "red",
		"size":  "large",
		"tags":  []string{"featured", "new"},
	}

	testData := map[string]interface{}{
		"name":        "Test Product",
		"description": "A test product description",
		"price":       29.99,
		"in_stock":    true,
		"metadata":    jsonData,
		"product_id":  "550e8400-e29b-41d4-a716-446655440000",
		"image":       []byte{0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A}, // PNG header
	}

	record, err := mgr.Insert(ctx, 1, "products", testData)
	if err != nil {
		t.Fatalf("insert record with advanced types: %v", err)
	}

	if record == nil {
		t.Fatal("expected record to be returned")
	}

	// Verify the data was stored correctly
	if record.Data["name"].Data != "Test Product" {
		t.Errorf("expected name 'Test Product', got %v", record.Data["name"].Data)
	}

	if record.Data["metadata"].Data == nil {
		t.Error("expected metadata to be stored")
	}

	if record.Data["product_id"].Data == nil {
		t.Error("expected product_id to be stored")
	}

	if record.Data["image"].Data == nil {
		t.Error("expected image to be stored")
	}
}

func TestViewManagement(t *testing.T) {
	mgr, teardown := setupTestManager(t)
	defer teardown()

	ctx := context.Background()
	tenantID := int64(1)

	t.Run("CreateAndViewView", func(t *testing.T) {
		viewName := "test_view"
		query := "SELECT * FROM test_table"

		// Create view
		err := mgr.CreateView(ctx, tenantID, viewName, query, false)
		require.NoError(t, err)

		// Get view definition
		viewDef, err := mgr.GetViewDefinition(ctx, tenantID, viewName)
		require.NoError(t, err)
		assert.Equal(t, viewName, viewDef.Name)
		assert.Equal(t, query, viewDef.Query)
	})

	t.Run("CreateOrReplaceView", func(t *testing.T) {
		viewName := "replace_view"
		query1 := "SELECT * FROM table1"
		query2 := "SELECT * FROM table2"

		// Create view
		err := mgr.CreateView(ctx, tenantID, viewName, query1, false)
		require.NoError(t, err)

		// Replace view
		err = mgr.CreateView(ctx, tenantID, viewName, query2, true)
		require.NoError(t, err)

		// Get view definition
		viewDef, err := mgr.GetViewDefinition(ctx, tenantID, viewName)
		require.NoError(t, err)
		assert.Equal(t, query2, viewDef.Query)
	})

	t.Run("DropView", func(t *testing.T) {
		viewName := "drop_view"
		query := "SELECT * FROM test_table"

		// Create view
		err := mgr.CreateView(ctx, tenantID, viewName, query, false)
		require.NoError(t, err)

		// Drop view
		err = mgr.DropView(ctx, tenantID, viewName)
		require.NoError(t, err)

		// Try to get dropped view
		_, err = mgr.GetViewDefinition(ctx, tenantID, viewName)
		assert.Error(t, err)
	})
}

func TestConstraintValidation(t *testing.T) {
	mgr, teardown := setupTestManager(t)
	defer teardown()

	ctx := context.Background()
	tenantID := int64(1)

	// Create a test table first
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{
				Name:     "id",
				Type:     types.ColumnTypeInteger,
				PrimaryKey: true,
			},
			{
				Name: "name",
				Type: types.ColumnTypeText,
			},
			{
				Name: "email",
				Type: types.ColumnTypeText,
			},
		},
	}

	err := mgr.CreateTable(ctx, tenantID, tableDef)
	require.NoError(t, err)

	t.Run("ValidateUniqueConstraint", func(t *testing.T) {
		constraint := &types.ConstraintDef{
			Name:    "unique_email",
			Type:    "unique",
			Columns: []string{"email"},
		}

		err := mgr.ValidateConstraint(ctx, tenantID, "test_table", constraint)
		assert.NoError(t, err)
	})

	t.Run("ValidateNonExistentColumn", func(t *testing.T) {
		constraint := &types.ConstraintDef{
			Name:    "invalid_constraint",
			Type:    "unique",
			Columns: []string{"nonexistent"},
		}

		err := mgr.ValidateConstraint(ctx, tenantID, "test_table", constraint)
		assert.Error(t, err)
	})
}

func TestAlterTableWithConstraints(t *testing.T) {
	mgr, teardown := setupTestManager(t)
	defer teardown()

	ctx := context.Background()
	tenantID := int64(1)

	// Create a test table first
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{
				Name: "id",
				Type: types.ColumnTypeInteger,
			},
			{
				Name: "name",
				Type: types.ColumnTypeText,
			},
		},
	}

	err := mgr.CreateTable(ctx, tenantID, tableDef)
	require.NoError(t, err)

	t.Run("AddColumn", func(t *testing.T) {
		changes := &AlterTableChanges{
			AddColumns: []types.ColumnDefinition{
				{
					Name: "email",
					Type: types.ColumnTypeText,
				},
			},
		}

		err := mgr.AlterTable(ctx, tenantID, "test_table", changes)
		assert.NoError(t, err)

		// Verify the column was added
		table, err := mgr.GetTableDefinition(ctx, tenantID, "test_table")
		require.NoError(t, err)
		assert.Len(t, table.Columns, 3)
		assert.Equal(t, "email", table.Columns[2].Name)
	})

	t.Run("ModifyColumnType", func(t *testing.T) {
		changes := &AlterTableChanges{
			ModifyColumns: []types.ColumnDefinition{
				{
					Name: "name",
					Type: types.ColumnTypeVarchar,
				},
			},
		}

		err := mgr.AlterTable(ctx, tenantID, "test_table", changes)
		assert.NoError(t, err)

		// Verify the column type was modified
		table, err := mgr.GetTableDefinition(ctx, tenantID, "test_table")
		require.NoError(t, err)
		for _, col := range table.Columns {
			if col.Name == "name" {
				assert.Equal(t, types.ColumnTypeVarchar, col.Type)
				break
			}
		}
	})

	t.Run("AddConstraint", func(t *testing.T) {
		changes := &AlterTableChanges{
			AddConstraints: []types.ConstraintDef{
				{
					Name:    "unique_name",
					Type:    "unique",
					Columns: []string{"name"},
				},
			},
		}

		err := mgr.AlterTable(ctx, tenantID, "test_table", changes)
		assert.NoError(t, err)

		// Verify the constraint was added
		table, err := mgr.GetTableDefinition(ctx, tenantID, "test_table")
		require.NoError(t, err)
		assert.Len(t, table.Constraints, 1)
		assert.Equal(t, "unique_name", table.Constraints[0].Name)
	})
}