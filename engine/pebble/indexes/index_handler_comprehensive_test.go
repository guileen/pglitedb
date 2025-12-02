package indexes

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestIndexHandler(t *testing.T) (*Handler, storage.KV, func()) {
	// Create an in-memory KV store for testing
	kv, err := storage.NewPebbleKV(storage.TestOptimizedPebbleConfig(""))
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	handler := NewHandler(kv, c)

	cleanup := func() {
		kv.Close()
	}

	return handler, kv, cleanup
}

func TestIndexHandler_CreateIndex(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Create a mock ID generator function
	nextIndexID := func(ctx context.Context, tenantID, tableID int64) (int64, error) {
		return 1, nil
	}

	// Test creating an index
	indexDef := &types.IndexDefinition{
		Name:    "idx_users_name",
		Columns: []string{"name"},
		Unique:  false,
	}

	err := handler.CreateIndex(ctx, 1, 1, indexDef, nextIndexID)
	assert.NoError(t, err)
}

func TestIndexHandler_DropIndex(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Test dropping an index (this will test the deletion logic)
	err := handler.DropIndex(ctx, 1, 1, 1)
	// Should not error even if index doesn't exist
	assert.NoError(t, err)
}

func TestIndexHandler_LookupIndex(t *testing.T) {
	handler, kv, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Manually create an index entry for testing
	c := codec.NewMemComparableCodec()
	indexKey, err := c.EncodeIndexKey(1, 1, 1, "test_value", 100)
	require.NoError(t, err)

	err = kv.Set(ctx, indexKey, []byte{})
	require.NoError(t, err)

	// Test looking up the index
	rowIDs, err := handler.LookupIndex(ctx, 1, 1, 1, "test_value")
	assert.NoError(t, err)
	assert.Len(t, rowIDs, 1)
	assert.Equal(t, int64(100), rowIDs[0])
}

func TestIndexHandler_LookupIndex_NoMatches(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Test looking up an index with no matches
	rowIDs, err := handler.LookupIndex(ctx, 1, 1, 1, "nonexistent_value")
	assert.NoError(t, err)
	assert.Empty(t, rowIDs)
}

func TestIndexHandler_UpdateIndexes_Insert(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Create a test record
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "John Doe", Type: types.ColumnTypeVarchar},
			"age":  {Data: int64(30), Type: types.ColumnTypeInteger},
		},
	}

	// Create a table schema with an index
	schemaDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false},
			{Name: "age", Type: types.ColumnTypeInteger, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_users_name", Columns: []string{"name"}, Unique: false},
		},
	}

	// Test updating indexes for insert
	err := handler.UpdateIndexes(ctx, 1, 1, 1, record, schemaDef, true)
	assert.NoError(t, err)
}

func TestIndexHandler_UpdateIndexes_Insert_UniqueConstraintViolation(t *testing.T) {
	handler, kv, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Manually create an existing index entry
	c := codec.NewMemComparableCodec()
	indexKey, err := c.EncodeIndexKey(1, 1, 1, "John Doe", 50)
	require.NoError(t, err)

	err = kv.Set(ctx, indexKey, []byte{})
	require.NoError(t, err)

	// Create a test record with the same value
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "John Doe", Type: types.ColumnTypeVarchar},
			"age":  {Data: int64(30), Type: types.ColumnTypeInteger},
		},
	}

	// Create a table schema with a unique index
	schemaDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false},
			{Name: "age", Type: types.ColumnTypeInteger, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_users_name", Columns: []string{"name"}, Unique: true},
		},
	}

	// Test updating indexes for insert with unique constraint violation
	err = handler.UpdateIndexes(ctx, 1, 1, 1, record, schemaDef, true)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unique constraint")
}

func TestIndexHandler_DeleteIndexes(t *testing.T) {
	handler, kv, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	ctx := context.Background()

	// Manually create an index entry
	c := codec.NewMemComparableCodec()
	indexKey, err := c.EncodeIndexKey(1, 1, 1, "test_value", 100)
	require.NoError(t, err)

	err = kv.Set(ctx, indexKey, []byte{})
	require.NoError(t, err)

	// Create a test record
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "test_value", Type: types.ColumnTypeVarchar},
		},
	}

	// Create a table schema with an index
	schemaDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_users_name", Columns: []string{"name"}, Unique: false},
		},
	}

	// Test deleting indexes
	err = handler.DeleteIndexes(ctx, 1, 1, 100, record, schemaDef)
	assert.NoError(t, err)

	// Verify the index entry was deleted
	iter := kv.NewIterator(&storage.IteratorOptions{
		LowerBound: indexKey,
		UpperBound: append(indexKey, 0xFF),
	})
	defer iter.Close()

	iter.First()
	assert.False(t, iter.Valid())
}

func TestIndexHandler_BatchUpdateIndexes(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	// Create a batch
	batch := &mockBatch{}

	// Create a test record
	record := &types.Record{
		Data: map[string]*types.Value{
			"name": {Data: "John Doe", Type: types.ColumnTypeVarchar},
		},
	}

	// Create a table schema with an index
	schemaDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_users_name", Columns: []string{"name"}, Unique: false},
		},
	}

	// Test batch updating indexes
	err := handler.BatchUpdateIndexes(batch, 1, 1, 1, record, schemaDef)
	assert.NoError(t, err)
	assert.Equal(t, 1, batch.setCalls)
}

func TestIndexHandler_BatchUpdateIndexesBulk(t *testing.T) {
	handler, _, cleanup := setupTestIndexHandler(t)
	defer cleanup()

	// Create a batch
	batch := &mockBatch{}

	// Create test records
	records := map[int64]*types.Record{
		1: {
			Data: map[string]*types.Value{
				"name": {Data: "John Doe", Type: types.ColumnTypeVarchar},
			},
		},
		2: {
			Data: map[string]*types.Value{
				"name": {Data: "Jane Smith", Type: types.ColumnTypeVarchar},
			},
		},
	}

	// Create a table schema with an index
	schemaDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar, Nullable: false},
		},
		Indexes: []types.IndexDefinition{
			{Name: "idx_users_name", Columns: []string{"name"}, Unique: false},
		},
	}

	// Test bulk batch updating indexes
	err := handler.BatchUpdateIndexesBulk(batch, 1, 1, records, schemaDef)
	assert.NoError(t, err)
	assert.Equal(t, 2, batch.setCalls)
}

// mockBatch implements storage.Batch for testing
type mockBatch struct {
	setCalls    int
	deleteCalls int
	count       int
}

func (m *mockBatch) Set(key, value []byte) error {
	m.setCalls++
	m.count++
	return nil
}

func (m *mockBatch) Delete(key []byte) error {
	m.deleteCalls++
	m.count++
	return nil
}

func (m *mockBatch) Count() int {
	return m.count
}

func (m *mockBatch) Reset() {
	m.setCalls = 0
	m.deleteCalls = 0
	m.count = 0
}

func (m *mockBatch) Close() error {
	return nil
}