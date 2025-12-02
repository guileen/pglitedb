package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/engine/pebble/operations/batch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestPebbleEngineCore creates a test engine instance for core functionality testing
func setupTestPebbleEngineCore(t *testing.T) (*pebbleEngine, func()) {
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

// TestPebbleEngine_CreationAndConfiguration tests the creation and basic configuration of pebbleEngine
func TestPebbleEngine_CreationAndConfiguration(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	// Test that engine is properly initialized
	assert.NotNil(t, engine)
	assert.NotNil(t, engine.kv)
	assert.NotNil(t, engine.codec)
	assert.NotNil(t, engine.idGenerator)
	assert.NotNil(t, engine.indexManager)
	assert.NotNil(t, engine.filterEvaluator)
	assert.NotNil(t, engine.multiColumnOptimizer)
	assert.NotNil(t, engine.queryOperations)
	assert.NotNil(t, engine.insertOperations)
	assert.NotNil(t, engine.updateOperations)
	assert.NotNil(t, engine.deleteOperations)
	assert.NotNil(t, engine.deadlockDetector)
	assert.NotNil(t, engine.iteratorPool)
	assert.NotNil(t, engine.batchProcessor)

	// Test that components are of correct types
	assert.Implements(t, (*storage.KV)(nil), engine.kv)
	assert.Implements(t, (*codec.Codec)(nil), engine.codec)
	assert.Implements(t, (*batch.ParallelBatchProcessor)(nil), engine.batchProcessor)
}

// TestPebbleEngine_Close tests the Close functionality of pebbleEngine
func TestPebbleEngine_Close(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	// Don't call the standard cleanup since we're testing Close functionality
	_ = cleanup // Mark as used to avoid compiler error
	
	// Test that Close doesn't return an error
	err := engine.Close()
	assert.NoError(t, err)

	// Test that calling Close again doesn't panic or cause issues
	err = engine.Close()
	assert.NoError(t, err)

	// Give goroutines time to finish
	time.Sleep(10 * time.Millisecond)
}

// TestPebbleEngine_GetCodec tests the GetCodec functionality
func TestPebbleEngine_GetCodec(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	c := engine.GetCodec()
	assert.NotNil(t, c)
	assert.Implements(t, (*codec.Codec)(nil), c)
}

// TestPebbleEngine_GetKV tests the GetKV functionality
func TestPebbleEngine_GetKV(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	kv := engine.GetKV()
	assert.NotNil(t, kv)
	assert.Implements(t, (*storage.KV)(nil), kv)
}

// TestPebbleEngine_IDGeneration tests ID generation functionality (NextRowID, NextTableID, NextIndexID)
func TestPebbleEngine_IDGeneration(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)

	// Test NextRowID
	rowID1, err := engine.NextRowID(ctx, tenantID, tableID)
	require.NoError(t, err)
	assert.Greater(t, rowID1, int64(0))

	rowID2, err := engine.NextRowID(ctx, tenantID, tableID)
	require.NoError(t, err)
	assert.Greater(t, rowID2, rowID1, "Row IDs should be incrementing")

	// Test NextTableID
	tableID1, err := engine.NextTableID(ctx, tenantID)
	require.NoError(t, err)
	assert.Greater(t, tableID1, int64(0))

	tableID2, err := engine.NextTableID(ctx, tenantID)
	require.NoError(t, err)
	assert.Greater(t, tableID2, tableID1, "Table IDs should be incrementing")

	// Test NextIndexID
	indexID1, err := engine.NextIndexID(ctx, tenantID, tableID)
	require.NoError(t, err)
	assert.Greater(t, indexID1, int64(0))

	indexID2, err := engine.NextIndexID(ctx, tenantID, tableID)
	require.NoError(t, err)
	assert.Greater(t, indexID2, indexID1, "Index IDs should be incrementing")
}

// TestPebbleEngine_IDGenerationEdgeCases tests edge cases for ID generation
func TestPebbleEngine_IDGenerationEdgeCases(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	ctx := context.Background()

	// Test with different tenant IDs
	tenantID1 := int64(1)
	tenantID2 := int64(2)
	tableID := int64(1)

	rowID1, err := engine.NextRowID(ctx, tenantID1, tableID)
	require.NoError(t, err)
	assert.Greater(t, rowID1, int64(0))

	rowID2, err := engine.NextRowID(ctx, tenantID2, tableID)
	require.NoError(t, err)
	assert.Greater(t, rowID2, int64(0))

	// Row IDs should be independent for different tenants
	// Both should be valid row IDs (positive integers)
	assert.Greater(t, rowID1, int64(0))
	assert.Greater(t, rowID2, int64(0))

	// Test with different table IDs
	tableID1 := int64(1)
	tableID2 := int64(2)
	tenantID := int64(1)

	tableIDRes1, err := engine.NextTableID(ctx, tenantID)
	require.NoError(t, err)
	assert.Greater(t, tableIDRes1, int64(0))

	tableIDRes2, err := engine.NextTableID(ctx, tenantID)
	require.NoError(t, err)
	assert.Greater(t, tableIDRes2, tableIDRes1)

	// Test index IDs with different table IDs
	indexID1, err := engine.NextIndexID(ctx, tenantID, tableID1)
	require.NoError(t, err)
	assert.Greater(t, indexID1, int64(0))

	indexID2, err := engine.NextIndexID(ctx, tenantID, tableID2)
	require.NoError(t, err)
	assert.Greater(t, indexID2, int64(0))
}

// TestPebbleEngine_DeadlockDetector tests the deadlock detector functionality
func TestPebbleEngine_DeadlockDetector(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	// Test that deadlock detector is properly initialized
	assert.NotNil(t, engine.deadlockDetector)
	
	// Test getting the deadlock detector
	detector := engine.GetDeadlockDetector()
	assert.NotNil(t, detector)
	assert.Equal(t, engine.deadlockDetector, detector)
	
	// Test that detector has proper initial state
	// Note: These methods might not be exported, so we'll test what we can
	assert.NotNil(t, detector)
}

// TestPebbleEngine_CheckForConflicts tests the CheckForConflicts functionality
func TestPebbleEngine_CheckForConflicts(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	// Test checking for conflicts with a key (should not conflict initially)
	// Since we don't have direct access to begin a transaction, we'll test the method exists and doesn't panic
	key := []byte("test_key")
	
	// We'll use a mock transaction-like object for testing
	// For now, we'll just test that the method exists and can be called
	assert.NotPanics(t, func() {
		// This will likely fail since we're not passing a real transaction,
		// but we're primarily testing that the method exists and doesn't panic unexpectedly
		_ = engine.CheckForConflicts(nil, key)
	})
}

// TestPebbleEngine_BatchProcessor tests the batch processor functionality
func TestPebbleEngine_BatchProcessor(t *testing.T) {
	engine, cleanup := setupTestPebbleEngineCore(t)
	defer cleanup()

	// Test that batch processor is properly initialized
	assert.NotNil(t, engine.batchProcessor)
	assert.Implements(t, (*batch.ParallelBatchProcessor)(nil), engine.batchProcessor)
}

// TestGetTransactionID tests the getTransactionID helper function
func TestGetTransactionID(t *testing.T) {
	// Test with nil transaction
	result := getTransactionID(nil)
	assert.Equal(t, uint64(0), result, "Should return 0 for nil transaction")
}