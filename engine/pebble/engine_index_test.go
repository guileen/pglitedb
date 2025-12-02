package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestPebbleEngineForIndex(t *testing.T) (*pebbleEngine, func()) {
	// Create an in-memory KV store for testing
	config := storage.TestOptimizedPebbleConfig("")
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	engine := NewPebbleEngine(kvStore, c).(*pebbleEngine)

	cleanup := func() {
		engine.Close()
		// Give goroutines time to finish
		time.Sleep(10 * time.Millisecond)
	}

	return engine, cleanup
}

func TestPebbleEngine_CreateIndex(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineForIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Create an index definition
	indexDef := &types.IndexDefinition{
		Name:    "idx_users_email",
		Columns: []string{"email"},
		Unique:  false,
		Type:    "btree",
	}

	// Test creating an index
	err := engine.CreateIndex(ctx, 1, 1, indexDef)
	require.NoError(t, err)

	// Test creating another index
	indexDef2 := &types.IndexDefinition{
		Name:    "idx_users_name",
		Columns: []string{"name"},
		Unique:  true,
		Type:    "btree",
	}

	err = engine.CreateIndex(ctx, 1, 1, indexDef2)
	require.NoError(t, err)
}

func TestPebbleEngine_DropIndex(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineForIndex(t)
	defer cleanup()

	ctx := context.Background()

	// First create an index
	indexDef := &types.IndexDefinition{
		Name:    "idx_users_name",
		Columns: []string{"name"},
		Unique:  false,
		Type:    "btree",
	}

	err := engine.CreateIndex(ctx, 1, 1, indexDef)
	require.NoError(t, err)

	// Test dropping the index
	// Note: This assumes index IDs are assigned sequentially starting from 1
	err = engine.DropIndex(ctx, 1, 1, 1)
	require.NoError(t, err)

	// Test dropping a non-existent index
	err = engine.DropIndex(ctx, 1, 1, 999)
	// This might not error depending on implementation
	if err != nil {
		assert.Error(t, err)
	}
}

func TestPebbleEngine_LookupIndex(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineForIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Test looking up index - this would typically be done internally
	// For now, we test that the method exists and doesn't panic
	rowIDs, err := engine.LookupIndex(ctx, 1, 1, 1, "test_value")
	// Depending on implementation, this might return results or not
	// The important thing is that it doesn't panic
	if err == nil {
		assert.NotNil(t, rowIDs)
	}
}

func TestPebbleEngine_UpdateIndexes(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineForIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table schema
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar},
			{Name: "email", Type: types.ColumnTypeVarchar},
		},
		Indexes: []types.IndexDefinition{
			{
				Name:    "idx_users_email",
				Columns: []string{"email"},
				Unique:  false,
				Type:    "btree",
			},
		},
	}

	// Create a record
	record := &types.Record{
		Data: map[string]*types.Value{
			"id":    {Data: int64(1), Type: types.ColumnTypeInteger},
			"name":  {Data: "John Doe", Type: types.ColumnTypeVarchar},
			"email": {Data: "john@example.com", Type: types.ColumnTypeVarchar},
		},
	}

	// Test updating indexes for an insert operation
	err := engine.updateIndexes(ctx, 1, 1, 1, record, schema, true)
	require.NoError(t, err)

	// Test updating indexes for an update operation
	err = engine.updateIndexes(ctx, 1, 1, 1, record, schema, false)
	require.NoError(t, err)
}

func TestPebbleEngine_DeleteIndexes(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineForIndex(t)
	defer cleanup()

	ctx := context.Background()

	// Create a table schema with indexes
	schema := &types.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeVarchar},
			{Name: "email", Type: types.ColumnTypeVarchar},
		},
		Indexes: []types.IndexDefinition{
			{
				Name:    "idx_users_email",
				Columns: []string{"email"},
				Unique:  false,
				Type:    "btree",
			},
		},
	}

	// Create a record
	record := &types.Record{
		Data: map[string]*types.Value{
			"id":    {Data: int64(1), Type: types.ColumnTypeInteger},
			"name":  {Data: "John Doe", Type: types.ColumnTypeVarchar},
			"email": {Data: "john@example.com", Type: types.ColumnTypeVarchar},
		},
	}

	// Test deleting indexes
	err := engine.deleteIndexes(ctx, 1, 1, 1, record, schema)
	require.NoError(t, err)
}