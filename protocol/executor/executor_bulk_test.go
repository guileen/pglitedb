package executor

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecutor_BulkUpdateDelete(t *testing.T) {
	// Setup test environment
	tmpDir := t.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	eng := engine.NewPebbleEngine(kvStore, c)
	defer eng.Close()

	// Create catalog manager
	manager := catalog.NewTableManager(eng)
	ctx := context.Background()

	// Create test table
	schema := &types.TableDefinition{
		Name:    "test_users",
		Version: 1,
		Columns: []types.ColumnDefinition{
			{Name: "name", Type: types.ColumnTypeString},
			{Name: "age", Type: types.ColumnTypeNumber},
			{Name: "city", Type: types.ColumnTypeString},
			{Name: "active", Type: types.ColumnTypeBoolean},
		},
	}
	err = manager.CreateTable(ctx, 1, schema)
	require.NoError(t, err)

	// Insert test data
	testData := []map[string]interface{}{
		{"name": "Alice", "age": int64(25), "city": "Beijing", "active": true},
		{"name": "Bob", "age": int64(30), "city": "Shanghai", "active": true},
		{"name": "Charlie", "age": int64(35), "city": "Beijing", "active": false},
		{"name": "David", "age": int64(28), "city": "Guangzhou", "active": true},
	}

	for _, data := range testData {
		_, err := manager.Insert(ctx, 1, "test_users", data)
		require.NoError(t, err)
	}

	// Test bulk UPDATE
	t.Run("BulkUpdate", func(t *testing.T) {
		// Create update query
		query := &Query{
			Type:      QueryTypeUpdate,
			TableName: "test_users",
			TenantID:  1,
			Update: &UpdateQuery{
				Values: map[string]*types.Value{
					"age": {Data: int64(30), Type: types.ColumnTypeNumber},
				},
				Where: []Filter{
					{Column: "city", Operator: OpEqual, Value: "Beijing"},
				},
			},
		}

		executor := NewExecutor(manager, eng)
		result, err := executor.Execute(ctx, query)
		require.NoError(t, err)
		assert.Equal(t, int64(2), result.Count) // Should update 2 records (Alice and Charlie)

		// Verify the updates
		opts := &types.QueryOptions{
			Where: map[string]interface{}{"city": "Beijing"},
		}
		queryResult, err := manager.Query(ctx, 1, "test_users", opts)
		require.NoError(t, err)
		
		records := queryResult.Records.([]*types.Record)
		assert.Len(t, records, 2)
		for _, record := range records {
			assert.Equal(t, int64(30), record.Data["age"].Data)
		}
	})

	// Test bulk DELETE
	t.Run("BulkDelete", func(t *testing.T) {
		// Create delete query
		query := &Query{
			Type:      QueryTypeDelete,
			TableName: "test_users",
			TenantID:  1,
			Delete: &DeleteQuery{
				Where: []Filter{
					{Column: "active", Operator: OpEqual, Value: false},
				},
			},
		}

		executor := NewExecutor(manager, eng)
		result, err := executor.Execute(ctx, query)
		require.NoError(t, err)
		assert.Equal(t, int64(1), result.Count) // Should delete 1 record (Charlie)

		// Verify the deletion
		allRecords, err := manager.Query(ctx, 1, "test_users", &types.QueryOptions{})
		require.NoError(t, err)
		
		records := allRecords.Records.([]*types.Record)
		assert.Len(t, records, 3) // Should have 3 records left (Alice, Bob, David)
	})
}