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