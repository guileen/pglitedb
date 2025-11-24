package manager

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/internal/codec"
	"github.com/guileen/pglitedb/internal/engine"
	"github.com/guileen/pglitedb/internal/kv"
	"github.com/guileen/pglitedb/internal/table"
)

func setupTestManager(t *testing.T) (Manager, func()) {
	tmpDir, err := os.MkdirTemp("", "manager-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	config := kv.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := kv.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create kv store: %v", err)
	}

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
	mgr := NewTableManager(eng)

	cleanup := func() {
		eng.Close()
		os.RemoveAll(tmpDir)
	}

	return mgr, cleanup
}

func createUsersTable() *table.TableDefinition {
	return &table.TableDefinition{
		Name: "users",
		Columns: []table.ColumnDefinition{
			{Name: "name", Type: table.ColumnTypeString, Nullable: false},
			{Name: "email", Type: table.ColumnTypeString, Nullable: false, Unique: true},
			{Name: "age", Type: table.ColumnTypeNumber, Nullable: true},
			{Name: "active", Type: table.ColumnTypeBoolean, Nullable: false},
		},
		Indexes: []table.IndexDefinition{
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
		opts := &table.QueryOptions{
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
		opts := &table.QueryOptions{
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

	config := kv.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := kv.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
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

	config := kv.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := kv.NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
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
	opts := &table.QueryOptions{Limit: &limit}

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
	tableDef := &table.TableDefinition{
		Name: "products",
		Columns: []table.ColumnDefinition{
			{Name: "name", Type: table.ColumnTypeString, Nullable: false},
			{Name: "description", Type: table.ColumnTypeText, Nullable: true},
			{Name: "price", Type: table.ColumnTypeNumber, Nullable: false},
			{Name: "in_stock", Type: table.ColumnTypeBoolean, Nullable: false},
			{Name: "metadata", Type: table.ColumnTypeJSON, Nullable: true},
			{Name: "product_id", Type: table.ColumnTypeUUID, Nullable: false},
			{Name: "image", Type: table.ColumnTypeBinary, Nullable: true},
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
	columnTypes := make(map[string]table.ColumnType)
	for _, col := range retrieved.Columns {
		columnTypes[col.Name] = col.Type
	}

	if columnTypes["metadata"] != table.ColumnTypeJSON {
		t.Errorf("expected metadata to be JSON type, got %s", columnTypes["metadata"])
	}

	if columnTypes["product_id"] != table.ColumnTypeUUID {
		t.Errorf("expected product_id to be UUID type, got %s", columnTypes["product_id"])
	}

	if columnTypes["image"] != table.ColumnTypeBinary {
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
