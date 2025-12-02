package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestPebbleEngine(t *testing.T) (engineTypes.StorageEngine, func()) {
	// Create an in-memory KV store for testing
	config := storage.TestOptimizedPebbleConfig("")
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	engine := NewPebbleEngine(kvStore, c)

	cleanup := func() {
		engine.Close()
		// Give goroutines time to finish
		time.Sleep(10 * time.Millisecond)
	}

	return engine, cleanup
}

func TestPebbleEngine_BeginTransaction(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Test beginning a transaction
	tx, err := engine.BeginTx(ctx)
	require.NoError(t, err)
	assert.NotNil(t, tx)

	// Test committing the transaction
	err = tx.Commit()
	assert.NoError(t, err)
}

func TestPebbleEngine_BeginTransaction_Rollback(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Test beginning a transaction
	tx, err := engine.BeginTx(ctx)
	require.NoError(t, err)
	assert.NotNil(t, tx)

	// Test rolling back the transaction
	err = tx.Rollback()
	assert.NoError(t, err)
}

func TestPebbleEngine_InsertAndRetrieve(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Create table schema
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
		},
	}

	// Insert a record directly on engine (not in transaction)
	record := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "John Doe", Type: types.ColumnTypeString},
		},
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	require.NoError(t, err)
	assert.Greater(t, rowID, int64(0))

	// Retrieve the record directly from engine (not in transaction)
	retrievedRecord, err := engine.GetRow(ctx, 1, 1, rowID, schema)
	require.NoError(t, err)
	assert.NotNil(t, retrievedRecord)
	assert.Equal(t, int64(1), retrievedRecord.Data["id"].Data)
	assert.Equal(t, "John Doe", retrievedRecord.Data["name"].Data)
}


func TestPebbleEngine_Scan(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Begin transaction
	tx, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Create table schema
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
		},
	}

	// Insert multiple records
	records := []*types.Record{
		{
			Data: map[string]*types.Value{
				"id":   {Data: int64(1), Type: types.ColumnTypeInteger},
				"name": {Data: "John Doe", Type: types.ColumnTypeVarchar},
			},
		},
		{
			Data: map[string]*types.Value{
				"id":   {Data: int64(2), Type: types.ColumnTypeInteger},
				"name": {Data: "Jane Smith", Type: types.ColumnTypeVarchar},
			},
		},
		{
			Data: map[string]*types.Value{
				"id":   {Data: int64(3), Type: types.ColumnTypeInteger},
				"name": {Data: "Bob Johnson", Type: types.ColumnTypeVarchar},
			},
		},
	}

	for _, record := range records {
		_, err = tx.InsertRow(ctx, 1, 1, record, schema)
		require.NoError(t, err)
	}

	// Commit transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Begin new transaction for scanning
	tx2, err := engine.BeginTx(ctx)
	require.NoError(t, err)
	defer tx2.Rollback()

	// Scan all records
	scanner, err := engine.ScanRows(ctx, 1, 1, schema, nil)
	require.NoError(t, err)
	defer scanner.Close()

	// Collect scanned records
	var scannedRecords []*types.Record
	for scanner.Next() {
		record := scanner.Row()
		require.NoError(t, err)
		scannedRecords = append(scannedRecords, record)
	}

	// Verify all records were scanned
	assert.Len(t, scannedRecords, 3)

	// Verify error handling
	err = scanner.Error()
	assert.NoError(t, err)
}

func TestPebbleEngine_Delete(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Begin transaction
	tx, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Create table schema
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
		},
	}

	// Insert a record
	record := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "John Doe", Type: types.ColumnTypeString},
		},
	}

	_, err = tx.InsertRow(ctx, 1, 1, record, schema)
	require.NoError(t, err)

	// Commit transaction
	err = tx.Commit()
	require.NoError(t, err)

	// Begin new transaction to delete the record
	tx2, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Delete the record
	err = tx2.DeleteRow(ctx, 1, 1, 1, schema)
	require.NoError(t, err)

	// Commit transaction
	err = tx2.Commit()
	require.NoError(t, err)

	// Verify record was deleted
	tx3, err := engine.BeginTx(ctx)
	require.NoError(t, err)
	defer tx3.Rollback()

	_, err = tx3.GetRow(ctx, 1, 1, 1, schema)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestPebbleEngine_Update(t *testing.T) {
	engine, cleanup := setupTestPebbleEngine(t)
	defer cleanup()

	ctx := context.Background()

	// Create table schema
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString},
		},
	}

	// Insert a record directly on engine
	record := &types.Record{
		Data: map[string]*types.Value{
			"id":   {Data: int64(1), Type: types.ColumnTypeNumber},
			"name": {Data: "John Doe", Type: types.ColumnTypeString},
		},
	}

	rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
	require.NoError(t, err)

	// Update the record directly on engine
	updates := map[string]*types.Value{
		"name": {Data: "Jane Smith", Type: types.ColumnTypeString},
	}

	err = engine.UpdateRow(ctx, 1, 1, rowID, updates, schema)
	require.NoError(t, err)

	// Verify record was updated
	retrievedRecord, err := engine.GetRow(ctx, 1, 1, rowID, schema)
	require.NoError(t, err)
	assert.Equal(t, "Jane Smith", retrievedRecord.Data["name"].Data)
}

func intPtr(i int) *int {
	return &i
}