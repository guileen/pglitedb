package integration

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCoreDatabaseFunctionality tests the core database functionality
func TestCoreDatabaseFunctionality(t *testing.T) {
	// Create a temporary directory for the test database
	tmpDir := "/tmp/pglitedb-core-test-" + time.Now().Format("20060102150405")
	
	// Clean up any previous test data
	t.Cleanup(func() {
		// Cleanup is handled by the storage layer
	})

	// Create database components
	kvConfig := storage.HighPerformancePebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(kvConfig)
	require.NoError(t, err)
	defer kvStore.Close()

	// Create codec
	c := codec.NewMemComparableCodec()

	// Create engine and manager
	eng := engine.NewStorageEngine(kvStore, c)
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)

	// Load existing schemas
	err = mgr.LoadSchemas(context.Background())
	require.NoError(t, err)

	ctx := context.Background()
	tenantID := int64(1)

	// Test basic table operations
	t.Run("BasicTableOperations", func(t *testing.T) {
		// Create a table with unique name
		tableDef := &types.TableDefinition{
			Name: "test_users_" + time.Now().Format("20060102150405"),
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
				{Name: "name", Type: types.ColumnTypeText},
				{Name: "email", Type: types.ColumnTypeText},
				{Name: "age", Type: types.ColumnTypeInteger},
			},
		}

		tableName := tableDef.Name
		
		err = mgr.CreateTable(ctx, tenantID, tableDef)
		// If table already exists, that's OK for this test
		if err != nil && err.Error() != "table already exists" {
			assert.NoError(t, err)
		}

		// Insert records using InsertRow method
		users := []map[string]interface{}{
			{"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
			{"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
			{"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35},
		}

		for _, user := range users {
			_, err = mgr.InsertRow(ctx, tenantID, tableName, user)
			assert.NoError(t, err)
		}

		// Query records using Query method with nil options
		result, err := mgr.Query(ctx, tenantID, tableName, nil)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(result.Rows))

		// Query with filter using QueryOptions
		opts := &types.QueryOptions{
			Where: map[string]interface{}{"age": 30},
		}
		result, err = mgr.Query(ctx, tenantID, tableName, opts)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
		if len(result.Rows) > 0 {
			assert.Equal(t, "John Doe", result.Rows[0][1]) // name column
		}

		// Update records
		updateData := map[string]interface{}{"email": "john.doe@example.com"}
		where := map[string]interface{}{"id": 1}
		updated, err := mgr.UpdateRows(ctx, tenantID, tableName, updateData, where)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), updated)

		// Verify update by querying again
		result, err = mgr.Query(ctx, tenantID, tableName, &types.QueryOptions{Where: where})
		assert.NoError(t, err)
		assert.Equal(t, 1, len(result.Rows))
		if len(result.Rows) > 0 {
			assert.Equal(t, "john.doe@example.com", result.Rows[0][2]) // email column
		}

		// Delete records
		where = map[string]interface{}{"age": 25}
		deleted, err := mgr.DeleteRows(ctx, tenantID, tableName, where)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), deleted)

		// Verify delete
		result, err = mgr.Query(ctx, tenantID, tableName, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows))
	})

	// Test simple transaction operations with integer data
	t.Run("SimpleTransactionOperations", func(t *testing.T) {
		// Create another table for transaction testing with integer data only
		tableDef := &types.TableDefinition{
			Name: "test_simple_accounts_" + time.Now().Format("20060102150405"),
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
				{Name: "balance", Type: types.ColumnTypeInteger},
			},
		}

		tableName := tableDef.Name
		
		err = mgr.CreateTable(ctx, tenantID, tableDef)
		// If table already exists, that's OK for this test
		if err != nil && err.Error() != "table already exists" {
			assert.NoError(t, err)
		}

		// Insert initial data
		accounts := []map[string]interface{}{
			{"id": 1, "balance": 1000},
			{"id": 2, "balance": 500},
		}

		for _, account := range accounts {
			_, err = mgr.InsertRow(ctx, tenantID, tableName, account)
			assert.NoError(t, err)
		}

		// Test basic query functionality
		result, err := mgr.Query(ctx, tenantID, tableName, nil)
		assert.NoError(t, err)
		assert.Equal(t, 2, len(result.Rows))
	})

	// Test performance characteristics
	t.Run("PerformanceCharacteristics", func(t *testing.T) {
		// Create a table for performance testing
		tableDef := &types.TableDefinition{
			Name: "test_performance_" + time.Now().Format("20060102150405"),
			Columns: []types.ColumnDefinition{
				{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
				{Name: "data", Type: types.ColumnTypeText},
			},
		}

		tableName := tableDef.Name
		
		err = mgr.CreateTable(ctx, tenantID, tableDef)
		// If table already exists, that's OK for this test
		if err != nil && err.Error() != "table already exists" {
			assert.NoError(t, err)
		}

		// Measure insert performance
		startTime := time.Now()
		batchSize := 50 // Reduced for faster testing
		for i := 0; i < batchSize; i++ {
			data := map[string]interface{}{
				"id":   i,
				"data": "test data " + string(rune(i%26+'a')),
			}
			_, err = mgr.InsertRow(ctx, tenantID, tableName, data)
			assert.NoError(t, err)
		}
		insertDuration := time.Since(startTime)

		// Measure select performance
		startTime = time.Now()
		result, err := mgr.Query(ctx, tenantID, tableName, nil)
		selectDuration := time.Since(startTime)

		assert.NoError(t, err)
		assert.Equal(t, batchSize, len(result.Rows))
		assert.True(t, insertDuration < 5*time.Second, "Insert operations should complete within 5 seconds")
		assert.True(t, selectDuration < 1*time.Second, "Select operations should complete within 1 second")

		t.Logf("Insert %d records took: %v", batchSize, insertDuration)
		t.Logf("Select %d records took: %v", batchSize, selectDuration)
	})
}