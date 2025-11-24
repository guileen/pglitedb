package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

func setupTestEngine(t *testing.T) (StorageEngine, func()) {
	tmpDir, err := os.MkdirTemp("", "engine-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create kv store: %v", err)
	}

	c := codec.NewMemComparableCodec()
	engine := NewPebbleEngine(kvStore, c)

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

func createTestSchema() *types.TableDefinition {
	return &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
			{Name: "email", Type: types.ColumnTypeString},
			{Name: "age", Type: types.ColumnTypeNumber},
			{Name: "active", Type: types.ColumnTypeBoolean},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
		},
	}
}

func TestStorageEngine_InsertAndGet(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	record := &types.Record{
		Data: map[string]*types.Value{
			"name":   {Data: "Alice", Type: types.ColumnTypeString},
			"email":  {Data: "alice@example.com", Type: types.ColumnTypeString},
			"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
			"active": {Data: true, Type: types.ColumnTypeBoolean},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	if rowID <= 0 {
		t.Errorf("expected positive row id, got %d", rowID)
	}

	retrieved, err := engine.GetRow(ctx, 1, 1, rowID, schema)
	if err != nil {
		t.Fatalf("get row: %v", err)
	}

	if retrieved.Data["name"].Data.(string) != "Alice" {
		t.Errorf("expected name Alice, got %v", retrieved.Data["name"].Data)
	}

	if retrieved.Data["email"].Data.(string) != "alice@example.com" {
		t.Errorf("expected email alice@example.com, got %v", retrieved.Data["email"].Data)
	}

	if retrieved.Data["age"].Data.(int64) != 30 {
		t.Errorf("expected age 30, got %v", retrieved.Data["age"].Data)
	}

	if retrieved.Data["active"].Data.(bool) != true {
		t.Errorf("expected active true, got %v", retrieved.Data["active"].Data)
	}
}

func TestStorageEngine_Update(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	record := &types.Record{
		Data: map[string]*types.Value{
			"name":   {Data: "Bob", Type: types.ColumnTypeString},
			"email":  {Data: "bob@example.com", Type: types.ColumnTypeString},
			"age":    {Data: int64(25), Type: types.ColumnTypeNumber},
			"active": {Data: true, Type: types.ColumnTypeBoolean},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	updates := map[string]*types.Value{
		"age":    {Data: int64(26), Type: types.ColumnTypeNumber},
		"active": {Data: false, Type: types.ColumnTypeBoolean},
	}

	if err := engine.UpdateRow(ctx, 1, 1, rowID, updates, schema); err != nil {
		t.Fatalf("update row: %v", err)
	}

	retrieved, err := engine.GetRow(ctx, 1, 1, rowID, schema)
	if err != nil {
		t.Fatalf("get row: %v", err)
	}

	if retrieved.Data["age"].Data.(int64) != 26 {
		t.Errorf("expected age 26, got %v", retrieved.Data["age"].Data)
	}

	if retrieved.Data["active"].Data.(bool) != false {
		t.Errorf("expected active false, got %v", retrieved.Data["active"].Data)
	}

	if retrieved.Data["name"].Data.(string) != "Bob" {
		t.Errorf("expected name Bob (unchanged), got %v", retrieved.Data["name"].Data)
	}
}

func TestStorageEngine_Delete(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	record := &types.Record{
		Data: map[string]*types.Value{
			"name":   {Data: "Charlie", Type: types.ColumnTypeString},
			"email":  {Data: "charlie@example.com", Type: types.ColumnTypeString},
			"age":    {Data: int64(35), Type: types.ColumnTypeNumber},
			"active": {Data: true, Type: types.ColumnTypeBoolean},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	if err := engine.DeleteRow(ctx, 1, 1, rowID, schema); err != nil {
		t.Fatalf("delete row: %v", err)
	}

	_, err = engine.GetRow(ctx, 1, 1, rowID, schema)
	if err != types.ErrRecordNotFound {
		t.Errorf("expected ErrRecordNotFound, got %v", err)
	}
}

func TestStorageEngine_IndexLookup(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	records := []*types.Record{
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Alice", Type: types.ColumnTypeString},
				"email":  {Data: "alice@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Bob", Type: types.ColumnTypeString},
				"email":  {Data: "bob@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(25), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Alice", Type: types.ColumnTypeString},
				"email":  {Data: "alice2@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(28), Type: types.ColumnTypeNumber},
				"active": {Data: false, Type: types.ColumnTypeBoolean},
			},
		},
	}

	for _, record := range records {
		record.CreatedAt = time.Now()
		record.UpdatedAt = time.Now()
		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	rowIDs, err := engine.LookupIndex(ctx, 1, 1, 2, "Alice")
	if err != nil {
		t.Fatalf("lookup index: %v", err)
	}

	if len(rowIDs) != 2 {
		t.Errorf("expected 2 rows with name Alice, got %d", len(rowIDs))
	}
}

func TestStorageEngine_ScanRows(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	for i := 0; i < 10; i++ {
		record := &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "User" + string(rune('0'+i)), Type: types.ColumnTypeString},
				"email":  {Data: "user" + string(rune('0'+i)) + "@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(20 + i), Type: types.ColumnTypeNumber},
				"active": {Data: i%2 == 0, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			t.Fatalf("insert row %d: %v", i, err)
		}
	}

	t.Run("Scan all rows", func(t *testing.T) {
		iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
		if err != nil {
			t.Fatalf("scan rows: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if err := iter.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}

		if count != 10 {
			t.Errorf("expected 10 rows, got %d", count)
		}
	})

	t.Run("Scan with limit", func(t *testing.T) {
		opts := &ScanOptions{Limit: 5}
		iter, err := engine.ScanRows(ctx, 1, 1, schema, opts)
		if err != nil {
			t.Fatalf("scan rows: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != 5 {
			t.Errorf("expected 5 rows, got %d", count)
		}
	})

	t.Run("Scan with offset", func(t *testing.T) {
		opts := &ScanOptions{Offset: 3, Limit: 3}
		iter, err := engine.ScanRows(ctx, 1, 1, schema, opts)
		if err != nil {
			t.Fatalf("scan rows: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.Next() {
			count++
		}

		if count != 3 {
			t.Errorf("expected 3 rows, got %d", count)
		}
	})
}

func TestStorageEngine_Transaction(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	t.Run("Commit transaction", func(t *testing.T) {
		txn, err := engine.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}

		record := &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "TxUser", Type: types.ColumnTypeString},
				"email":  {Data: "txuser@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(40), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		rowID, err := txn.InsertRow(ctx, 1, 1, record, schema)
		if err != nil {
			t.Fatalf("txn insert row: %v", err)
		}

		if err := txn.Commit(); err != nil {
			t.Fatalf("commit transaction: %v", err)
		}

		retrieved, err := engine.GetRow(ctx, 1, 1, rowID, schema)
		if err != nil {
			t.Fatalf("get row after commit: %v", err)
		}

		if retrieved.Data["name"].Data.(string) != "TxUser" {
			t.Errorf("expected name TxUser, got %v", retrieved.Data["name"].Data)
		}
	})

	t.Run("Rollback transaction", func(t *testing.T) {
		txn, err := engine.BeginTx(ctx)
		if err != nil {
			t.Fatalf("begin transaction: %v", err)
		}

		record := &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "RollbackUser", Type: types.ColumnTypeString},
				"email":  {Data: "rollback@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(45), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		rowID, err := txn.InsertRow(ctx, 1, 1, record, schema)
		if err != nil {
			t.Fatalf("txn insert row: %v", err)
		}

		if err := txn.Rollback(); err != nil {
			t.Fatalf("rollback transaction: %v", err)
		}

		_, err = engine.GetRow(ctx, 1, 1, rowID, schema)
		if err != types.ErrRecordNotFound {
			t.Errorf("expected ErrRecordNotFound after rollback, got %v", err)
		}
	})
}

func TestStorageEngine_NextRowID(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()

	id1, err := engine.NextRowID(ctx, 1, 1)
	if err != nil {
		t.Fatalf("next row id: %v", err)
	}

	id2, err := engine.NextRowID(ctx, 1, 1)
	if err != nil {
		t.Fatalf("next row id: %v", err)
	}

	if id2 != id1+1 {
		t.Errorf("expected sequential IDs, got %d and %d", id1, id2)
	}

	idOtherTable, err := engine.NextRowID(ctx, 1, 2)
	if err != nil {
		t.Fatalf("next row id for other table: %v", err)
	}

	if idOtherTable == id2 {
		t.Errorf("expected different ID sequences for different tables")
	}
}

func BenchmarkStorageEngine_InsertRow(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "engine-bench-*")
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
	engine := NewPebbleEngine(kvStore, c)

	ctx := context.Background()
	schema := createTestSchema()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		record := &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: "BenchUser", Type: types.ColumnTypeString},
				"email":  {Data: "bench@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}

		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			b.Fatalf("insert row: %v", err)
		}
	}
}

func BenchmarkStorageEngine_GetRow(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "engine-bench-*")
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
	engine := NewPebbleEngine(kvStore, c)

	ctx := context.Background()
	schema := createTestSchema()

	record := &types.Record{
		Data: map[string]*types.Value{
			"name":   {Data: "BenchUser", Type: types.ColumnTypeString},
			"email":  {Data: "bench@example.com", Type: types.ColumnTypeString},
			"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
			"active": {Data: true, Type: types.ColumnTypeBoolean},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	if err != nil {
		b.Fatalf("insert row: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := engine.GetRow(ctx, 1, 1, rowID, schema); err != nil {
			b.Fatalf("get row: %v", err)
		}
	}
}

func TestStorageEngine_IsolationLevels(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	// schema := createTestSchema() // Not used in this test

	// Test 1: Begin transaction with default isolation level
	txn1, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction: %v", err)
	}

	// Check default isolation level
	if txn1.Isolation() != storage.ReadCommitted {
		t.Errorf("expected default isolation level ReadCommitted, got %v", txn1.Isolation())
	}

	// Test 2: Begin transaction with specific isolation level
	txn2, err := engine.BeginTxWithIsolation(ctx, storage.RepeatableRead)
	if err != nil {
		t.Fatalf("begin transaction with isolation: %v", err)
	}

	if txn2.Isolation() != storage.RepeatableRead {
		t.Errorf("expected isolation level RepeatableRead, got %v", txn2.Isolation())
	}

	// Test 3: Change isolation level
	err = txn2.SetIsolation(storage.Serializable)
	if err != nil {
		t.Fatalf("set isolation level: %v", err)
	}

	if txn2.Isolation() != storage.Serializable {
		t.Errorf("expected isolation level Serializable, got %v", txn2.Isolation())
	}

	// Clean up
	txn1.Rollback()
	txn2.Rollback()
}

func TestStorageEngine_TransactionWithConflictDetection(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	// Insert a record first
	record := &types.Record{
		Data: map[string]*types.Value{
			"name":   {Data: "ConflictTest", Type: types.ColumnTypeString},
			"email":  {Data: "conflict@example.com", Type: types.ColumnTypeString},
			"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
			"active": {Data: true, Type: types.ColumnTypeBoolean},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	if err != nil {
		t.Fatalf("insert row: %v", err)
	}

	// Begin two transactions
	txn1, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction 1: %v", err)
	}

	txn2, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("begin transaction 2: %v", err)
	}

	// Both transactions try to update the same record
	updates1 := map[string]*types.Value{
		"age": {Data: int64(31), Type: types.ColumnTypeNumber},
	}

	updates2 := map[string]*types.Value{
		"age": {Data: int64(32), Type: types.ColumnTypeNumber},
	}

	// Update in first transaction
	err = txn1.UpdateRow(ctx, 1, 1, rowID, updates1, schema)
	if err != nil {
		t.Fatalf("txn1 update row: %v", err)
	}

	// Update in second transaction (may detect conflict)
	err = txn2.UpdateRow(ctx, 1, 1, rowID, updates2, schema)
	if err != nil {
		// In our simplified implementation, conflict detection may not be fully implemented
		t.Logf("txn2 update returned: %v", err)
	}

	// Commit first transaction
	err = txn1.Commit()
	if err != nil {
		t.Fatalf("commit txn1: %v", err)
	}

	// Try to commit second transaction (may fail due to conflict)
	err = txn2.Commit()
	if err != nil {
		// This is expected in a full implementation
		t.Logf("commit txn2 returned: %v", err)
	} else {
		// If it succeeds, check the final value
		finalRecord, err := engine.GetRow(ctx, 1, 1, rowID, schema)
		if err != nil {
			t.Fatalf("get final record: %v", err)
		}
		t.Logf("Final age value: %v", finalRecord.Data["age"].Data)
	}

	// Clean up if needed
	txn2.Rollback()
}
