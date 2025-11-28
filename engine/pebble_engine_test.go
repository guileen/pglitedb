package engine

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	engineTypes "github.com/guileen/pglitedb/engine/types"
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
	engine := pebble.NewPebbleEngine(kvStore, c)

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
		opts := &engineTypes.ScanOptions{Limit: 5}
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
		opts := &engineTypes.ScanOptions{Offset: 3, Limit: 3}
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
	engine := pebble.NewPebbleEngine(kvStore, c)

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
	engine := pebble.NewPebbleEngine(kvStore, c)

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

func TestStorageEngine_PerformanceOptimizations(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	// Insert a large number of records to test batch operations
	const recordCount = 1000
	records := make([]*types.Record, recordCount)
	
	for i := 0; i < recordCount; i++ {
		records[i] = &types.Record{
			Data: map[string]*types.Value{
				"name":   {Data: fmt.Sprintf("User%d", i), Type: types.ColumnTypeString},
				"email":  {Data: fmt.Sprintf("user%d@example.com", i), Type: types.ColumnTypeString},
				"age":    {Data: int64(20 + (i % 50)), Type: types.ColumnTypeNumber},
				"active": {Data: i%3 == 0, Type: types.ColumnTypeBoolean}, // Every 3rd user is active
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
	}

	// Test batch insert performance
	start := time.Now()
	_, err := engine.InsertRowBatch(ctx, 1, 1, records, schema)
	if err != nil {
		t.Fatalf("insert row batch: %v", err)
	}
	batchInsertDuration := time.Since(start)

	// Verify all records were inserted
	iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	
	count := 0
	for iter.Next() {
		count++
	}
	iter.Close()

	if count != recordCount {
		t.Errorf("expected %d records, got %d", recordCount, count)
	}

	// Test UpdateRows performance optimization
	updates := map[string]*types.Value{
		"age": {Data: int64(99), Type: types.ColumnTypeNumber},
	}

	conditions := map[string]interface{}{
		"active": true,
	}

	start = time.Now()
	affected, err := engine.UpdateRows(ctx, 1, 1, updates, conditions, schema)
	if err != nil {
		t.Fatalf("update rows: %v", err)
	}
	updateRowsDuration := time.Since(start)

	// Calculate expected affected rows (every 3rd user is active: 0, 3, 6, 9, ...)
	expectedAffected := 0
	for i := 0; i < recordCount; i++ {
		if i%3 == 0 {
			expectedAffected++
		}
	}
	
	if int(affected) != expectedAffected {
		t.Errorf("expected %d affected rows, got %d", expectedAffected, affected)
	}

	// Test DeleteRows performance optimization
	deleteConditions := map[string]interface{}{
		"age": int64(99), // Delete users whose age was just updated to 99
	}

	start = time.Now()
	deleted, err := engine.DeleteRows(ctx, 1, 1, deleteConditions, schema)
	if err != nil {
		t.Fatalf("delete rows: %v", err)
	}
	deleteRowsDuration := time.Since(start)

	if deleted != affected {
		t.Errorf("expected %d deleted rows, got %d", affected, deleted)
	}

	// Log performance metrics (for manual verification)
	t.Logf("Batch insert of %d records took: %v", recordCount, batchInsertDuration)
	t.Logf("UpdateRows of %d records took: %v", affected, updateRowsDuration)
	t.Logf("DeleteRows of %d records took: %v", deleted, deleteRowsDuration)
}

func TestStorageEngine_UpdateRows(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	// Insert test data
	records := []*types.Record{
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Alice", Type: types.ColumnTypeString},
				"email":  {Data: "alice@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Bob", Type: types.ColumnTypeString},
				"email":  {Data: "bob@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(25), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Charlie", Type: types.ColumnTypeString},
				"email":  {Data: "charlie@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(35), Type: types.ColumnTypeNumber},
				"active": {Data: false, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	for _, record := range records {
		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	// Test case 1: Update rows with matching conditions
	updates := map[string]*types.Value{
		"age": {Data: int64(40), Type: types.ColumnTypeNumber},
	}

	conditions := map[string]interface{}{
		"active": true,
	}

	affected, err := engine.UpdateRows(ctx, 1, 1, updates, conditions, schema)
	if err != nil {
		t.Fatalf("update rows: %v", err)
	}

	if affected != 2 {
		t.Errorf("expected 2 affected rows, got %d", affected)
	}

	// Verify updates were applied
	iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	defer iter.Close()

	updatedCount := 0
	for iter.Next() {
		record := iter.Row()
		if record.Data["active"].Data.(bool) == true {
			if record.Data["age"].Data.(int64) != 40 {
				t.Errorf("expected age 40 for active user, got %v", record.Data["age"].Data)
			}
			updatedCount++
		}
	}

	if updatedCount != 2 {
		t.Errorf("expected 2 updated records, got %d", updatedCount)
	}
}

func TestStorageEngine_DeleteRows(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	// Insert test data
	records := []*types.Record{
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Alice", Type: types.ColumnTypeString},
				"email":  {Data: "alice@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Bob", Type: types.ColumnTypeString},
				"email":  {Data: "bob@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(25), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Charlie", Type: types.ColumnTypeString},
				"email":  {Data: "charlie@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(35), Type: types.ColumnTypeNumber},
				"active": {Data: false, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	for _, record := range records {
		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	// Test case 1: Delete rows with matching conditions
	conditions := map[string]interface{}{
		"active": true,
	}

	deleted, err := engine.DeleteRows(ctx, 1, 1, conditions, schema)
	if err != nil {
		t.Fatalf("delete rows: %v", err)
	}

	if deleted != 2 {
		t.Errorf("expected 2 deleted rows, got %d", deleted)
	}

	// Verify only 1 record remains
	iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if count != 1 {
		t.Errorf("expected 1 remaining record, got %d", count)
	}
}

func TestStorageEngine_BoundaryCases(t *testing.T) {
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schema := createTestSchema()

	// Test empty conditions for UpdateRows (should update all rows)
	records := []*types.Record{
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Alice", Type: types.ColumnTypeString},
				"email":  {Data: "alice@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(30), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
		{
			Data: map[string]*types.Value{
				"name":   {Data: "Bob", Type: types.ColumnTypeString},
				"email":  {Data: "bob@example.com", Type: types.ColumnTypeString},
				"age":    {Data: int64(25), Type: types.ColumnTypeNumber},
				"active": {Data: true, Type: types.ColumnTypeBoolean},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		},
	}

	for _, record := range records {
		if _, err := engine.InsertRow(ctx, 1, 1, record, schema); err != nil {
			t.Fatalf("insert row: %v", err)
		}
	}

	// Test UpdateRows with empty conditions (should affect all rows)
	updates := map[string]*types.Value{
		"age": {Data: int64(50), Type: types.ColumnTypeNumber},
	}

	emptyConditions := map[string]interface{}{}

	affected, err := engine.UpdateRows(ctx, 1, 1, updates, emptyConditions, schema)
	if err != nil {
		t.Fatalf("update rows with empty conditions: %v", err)
	}

	if affected != 2 {
		t.Errorf("expected 2 affected rows with empty conditions, got %d", affected)
	}

	// Test DeleteRows with empty conditions (should delete all rows)
	deleted, err := engine.DeleteRows(ctx, 1, 1, emptyConditions, schema)
	if err != nil {
		t.Fatalf("delete rows with empty conditions: %v", err)
	}

	if deleted != 2 {
		t.Errorf("expected 2 deleted rows with empty conditions, got %d", deleted)
	}

	// Verify no records remain
	iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
	if err != nil {
		t.Fatalf("scan rows: %v", err)
	}
	defer iter.Close()

	count := 0
	for iter.Next() {
		count++
	}

	if count != 0 {
		t.Errorf("expected 0 remaining records, got %d", count)
	}
}