package transactions

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/transactions/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestEngine(t *testing.T) (engineTypes.StorageEngine, func()) {
	tmpDir, err := os.MkdirTemp("", "snapshot-test-*")
	require.NoError(t, err)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	// Instead of importing the pebble package directly, we'll create a minimal engine implementation for tests
	// This avoids the import cycle
	engine := &mockEngine{
		kv:   kvStore,
		codec: c,
	}

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

func createTestSchema() *types.TableDefinition {
	return &types.TableDefinition{
		ID:   "1",
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
			{Name: "age", Type: types.ColumnTypeNumber},
		},
	}
}

func TestSnapshotTransaction_UpdateRows(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Insert some test data and store the row IDs
	rowIDs := make([]int64, 0, 3)
	for i := int64(1); i <= 3; i++ {
		row := &types.Record{
			Data: map[string]*types.Value{
				"id":   {Data: i, Type: types.ColumnTypeNumber},
				"name": {Data: "user" + string(rune('0'+int(i))), Type: types.ColumnTypeString},
				"age":  {Data: 20 + int64(i), Type: types.ColumnTypeNumber},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		rowID, err := engine.InsertRow(ctx, 1, 1, row, schemaDef)
		require.NoError(t, err)
		rowIDs = append(rowIDs, rowID)
	}

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test updating rows with a condition
	updates := map[string]*types.Value{
		"age": {Data: int64(30), Type: types.ColumnTypeNumber},
	}

	conditions := map[string]interface{}{
		"age": int64(21),
	}

	// Update rows that match the condition
	count, err := tx.UpdateRows(ctx, 1, 1, updates, conditions, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Commit the transaction to apply changes
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the update by getting the row through the engine
	// The row with age 21 should now have age 30
	record, err := engine.GetRow(ctx, 1, 1, rowIDs[0], schemaDef) // rowIDs[0] corresponds to the first inserted row
	require.NoError(t, err)
	assert.Equal(t, int64(30), record.Data["age"].Data)

	// Verify other rows are unchanged
	record, err = engine.GetRow(ctx, 1, 1, rowIDs[1], schemaDef) // rowIDs[1] corresponds to the second inserted row
	require.NoError(t, err)
	assert.Equal(t, int64(22), record.Data["age"].Data)
}

func TestSnapshotTransaction_GetRow(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Insert a test row
	row := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "user1", Type: types.ColumnTypeString},
			"age":  {Data: 21, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	rowID, err := engine.InsertRow(ctx, 1, 1, row, schemaDef)
	require.NoError(t, err)

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test getting the row through the transaction
	record, err := tx.GetRow(ctx, 1, 1, rowID, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, "user1", record.Data["name"].Data)
	assert.Equal(t, int64(21), record.Data["age"].Data)
}

func TestSnapshotTransaction_InsertRow(t *testing.T) {
	// Create a test engine
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test inserting a row through the transaction
	row := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "newuser", Type: types.ColumnTypeString},
			"age":  {Data: 25, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}

	rowID, err := tx.InsertRow(ctx, 1, 1, row, schemaDef)
	require.NoError(t, err)
	assert.Greater(t, rowID, int64(0))

	// Commit the transaction to apply changes
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the insertion by getting the row through the engine
	record, err := engine.GetRow(ctx, 1, 1, rowID, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, "newuser", record.Data["name"].Data)
	assert.Equal(t, int64(25), record.Data["age"].Data)
}

func TestSnapshotTransaction_UpdateRow(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Insert a test row
	row := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "user1", Type: types.ColumnTypeString},
			"age":  {Data: 21, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	rowID, err := engine.InsertRow(ctx, 1, 1, row, schemaDef)
	require.NoError(t, err)

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test updating the row through the transaction
	updates := map[string]*types.Value{
		"name": {Data: "updateduser", Type: types.ColumnTypeString},
		"age":  {Data: int64(30), Type: types.ColumnTypeNumber},
	}

	err = tx.UpdateRow(ctx, 1, 1, rowID, updates, schemaDef)
	require.NoError(t, err)

	// Commit the transaction to apply changes
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the update by getting the row through the engine
	record, err := engine.GetRow(ctx, 1, 1, rowID, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, "updateduser", record.Data["name"].Data)
	assert.Equal(t, int64(30), record.Data["age"].Data)
}

func TestSnapshotTransaction_DeleteRow(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Insert a test row
	row := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "user1", Type: types.ColumnTypeString},
			"age":  {Data: 21, Type: types.ColumnTypeNumber},
		},
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	rowID, err := engine.InsertRow(ctx, 1, 1, row, schemaDef)
	require.NoError(t, err)

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test deleting the row through the transaction
	err = tx.DeleteRow(ctx, 1, 1, rowID, schemaDef)
	require.NoError(t, err)

	// Commit the transaction to apply changes
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the deletion by trying to get the row through the engine
	_, err = engine.GetRow(ctx, 1, 1, rowID, schemaDef)
	assert.Error(t, err)
	assert.Equal(t, types.ErrRecordNotFound, err)
}

func TestSnapshotTransaction_ErrorConditions(t *testing.T) {
	// Create a test engine
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test operations on a closed transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Try to perform operations on a closed transaction
	_, err = tx.GetRow(ctx, 1, 1, 1, schemaDef)
	assert.Error(t, err)
	assert.True(t, errors.IsClosedError(err))

	_, err = tx.InsertRow(ctx, 1, 1, &types.Record{}, schemaDef)
	assert.Error(t, err)
	assert.True(t, errors.IsClosedError(err))

	err = tx.UpdateRow(ctx, 1, 1, 1, nil, schemaDef)
	assert.Error(t, err)
	assert.True(t, errors.IsClosedError(err))

	err = tx.DeleteRow(ctx, 1, 1, 1, schemaDef)
	assert.Error(t, err)
	assert.True(t, errors.IsClosedError(err))

	// Test isolation level setting (should fail for snapshot transactions)
	newSnapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	tx2 := NewSnapshotTransaction(engine, newSnapshot)
	err = tx2.SetIsolation(storage.ReadCommitted)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "cannot change isolation level after transaction started")
}

func TestSnapshotTransaction_DeleteRows(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createTestSchema()

	// Insert some test data and store the row IDs
	rowIDs := make([]int64, 0, 3)
	for i := int64(1); i <= 3; i++ {
		row := &types.Record{
			Data: map[string]*types.Value{
				"id":   {Data: i, Type: types.ColumnTypeNumber},
				"name": {Data: "user" + string(rune('0'+int(i))), Type: types.ColumnTypeString},
				"age":  {Data: 20 + int64(i), Type: types.ColumnTypeNumber},
			},
			CreatedAt: time.Now(),
			UpdatedAt: time.Now(),
		}
		rowID, err := engine.InsertRow(ctx, 1, 1, row, schemaDef)
		require.NoError(t, err)
		rowIDs = append(rowIDs, rowID)
	}

	// Create a snapshot
	snapshot, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewSnapshot()
	require.NoError(t, err)
	// Note: The snapshot will be closed by the transaction's Commit/Rollback methods

	// Create a snapshot transaction
	tx := NewSnapshotTransaction(engine, snapshot)

	// Test deleting rows with a condition
	conditions := map[string]interface{}{
		"age": int64(21),
	}

	// Delete rows that match the condition
	count, err := tx.DeleteRows(ctx, 1, 1, conditions, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, int64(1), count)

	// Commit the transaction to apply changes
	err = tx.Commit()
	require.NoError(t, err)

	// Verify the deletion by trying to get the row through the engine
	_, err = engine.GetRow(ctx, 1, 1, rowIDs[0], schemaDef)
	assert.Error(t, err)
	assert.Equal(t, types.ErrRecordNotFound, err)

	// Verify other rows still exist
	_, err = engine.GetRow(ctx, 1, 1, rowIDs[1], schemaDef)
	require.NoError(t, err)
}