package transactions

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupRegularTestEngine(t *testing.T) (engineTypes.StorageEngine, func()) {
	tmpDir, err := os.MkdirTemp("", "regular-test-*")
	require.NoError(t, err)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	engine := pebble.NewPebbleEngine(kvStore, c)

	cleanup := func() {
		engine.Close()
		os.RemoveAll(tmpDir)
	}

	return engine, cleanup
}

func createRegularTestSchema() *types.TableDefinition {
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

func TestRegularTransaction_GetRow(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupRegularTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createRegularTestSchema()

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

	// Begin a regular transaction
	kvTxn, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewTransaction(ctx)
	require.NoError(t, err)

	c := engine.GetCodec()
	tx := NewRegularTransaction(engine, kvTxn, c)

	// Test getting the row through the transaction
	record, err := tx.GetRow(ctx, 1, 1, rowID, schemaDef)
	require.NoError(t, err)
	assert.Equal(t, "user1", record.Data["name"].Data)
	assert.Equal(t, int64(21), record.Data["age"].Data)
}

func TestRegularTransaction_UpdateRows(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupRegularTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createRegularTestSchema()

	// Insert some test data
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

	// Begin a regular transaction
	kvTxn, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewTransaction(ctx)
	require.NoError(t, err)

	c := engine.GetCodec()
	tx := NewRegularTransaction(engine, kvTxn, c)

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
	record, err := engine.GetRow(ctx, 1, 1, rowIDs[0], schemaDef)
	require.NoError(t, err)
	assert.Equal(t, int64(30), record.Data["age"].Data)

	// Verify other rows are unchanged
	record, err = engine.GetRow(ctx, 1, 1, rowIDs[1], schemaDef)
	require.NoError(t, err)
	assert.Equal(t, int64(22), record.Data["age"].Data)
}

func TestRegularTransaction_DeleteRows(t *testing.T) {
	// Create a test engine and insert some data
	engine, cleanup := setupRegularTestEngine(t)
	defer cleanup()

	ctx := context.Background()
	schemaDef := createRegularTestSchema()

	// Insert some test data
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

	// Begin a regular transaction
	kvTxn, err := engine.(interface{ GetKV() storage.KV }).GetKV().NewTransaction(ctx)
	require.NoError(t, err)

	c := engine.GetCodec()
	tx := NewRegularTransaction(engine, kvTxn, c)

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