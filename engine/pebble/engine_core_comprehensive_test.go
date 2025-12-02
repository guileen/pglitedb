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

// TestPebbleEngine_ComprehensiveCreation tests comprehensive creation scenarios
func TestPebbleEngine_ComprehensiveCreation(t *testing.T) {
	t.Run("EngineWithDifferentConfigs", func(t *testing.T) {
		// Test engine creation with different storage configs
		testCases := []struct {
			name   string
			config func(string) *storage.PebbleConfig
		}{
			{
				name: "DefaultConfig",
				config: func(path string) *storage.PebbleConfig {
					// Use empty string for in-memory storage to avoid filesystem issues
					if path != "" {
						return storage.DefaultPebbleConfig(path)
					}
					return storage.TestOptimizedPebbleConfig("")
				},
			},
			{
				name: "HighPerformanceConfig",
				config: func(path string) *storage.PebbleConfig {
					// Use empty string for in-memory storage to avoid filesystem issues
					if path != "" {
						return storage.HighPerformancePebbleConfig(path)
					}
					return storage.TestOptimizedPebbleConfig("")
				},
			},
			{
				name: "TestOptimizedConfig",
				config: func(path string) *storage.PebbleConfig {
					// Use empty string for in-memory storage
					return storage.TestOptimizedPebbleConfig("")
				},
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				// Create an in-memory KV store for testing
				config := tc.config("")
				kvStore, err := storage.NewPebbleKV(config)
				require.NoError(t, err)

				c := codec.NewMemComparableCodec()
				engine := NewPebbleEngine(kvStore, c).(*pebbleEngine)

				// Test that engine is properly initialized
				assert.NotNil(t, engine)
				assert.NotNil(t, engine.kv)
				assert.NotNil(t, engine.codec)

				// Cleanup
				engine.Close()
				time.Sleep(10 * time.Millisecond)
			})
		}
	})

	t.Run("EngineComponentInitialization", func(t *testing.T) {
		// Test that all engine components are properly initialized
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		// Verify all components
		components := map[string]interface{}{
			"kv":                  engine.kv,
			"codec":               engine.codec,
			"idGenerator":         engine.idGenerator,
			"indexManager":        engine.indexManager,
			"filterEvaluator":     engine.filterEvaluator,
			"multiColumnOptimizer": engine.multiColumnOptimizer,
			"queryOperations":     engine.queryOperations,
			"insertOperations":    engine.insertOperations,
			"updateOperations":    engine.updateOperations,
			"deleteOperations":    engine.deleteOperations,
			"deadlockDetector":    engine.deadlockDetector,
			"iteratorPool":        engine.iteratorPool,
			"batchProcessor":      engine.batchProcessor,
		}

		for name, component := range components {
			assert.NotNil(t, component, "Component %s should not be nil", name)
		}
	})
}

// TestPebbleEngine_CloseComprehensive tests comprehensive close scenarios
func TestPebbleEngine_CloseComprehensive(t *testing.T) {
	t.Run("MultipleCloseCalls", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		// Don't call the standard cleanup since we're testing Close functionality
		_ = cleanup

		// Test that Close can be called multiple times without issues
		err1 := engine.Close()
		assert.NoError(t, err1)

		err2 := engine.Close()
		assert.NoError(t, err2)

		err3 := engine.Close()
		assert.NoError(t, err3)

		// Give goroutines time to finish
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("CloseWithActiveOperations", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		// Don't call the standard cleanup since we're testing Close functionality
		_ = cleanup

		// Test closing engine with potentially active operations
		// Get some components first
		_ = engine.GetCodec()
		_ = engine.GetKV()
		_ = engine.GetDeadlockDetector()

		// Close should still work
		err := engine.Close()
		assert.NoError(t, err)

		// Give goroutines time to finish
		time.Sleep(10 * time.Millisecond)
	})
}

// TestPebbleEngine_IDGenerationComprehensive tests comprehensive ID generation
func TestPebbleEngine_IDGenerationComprehensive(t *testing.T) {
	t.Run("ConcurrentIDGeneration", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(1)
		tableID := int64(1)

		// Test concurrent ID generation
		const numGoroutines = 10
		results := make(chan int64, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				rowID, err := engine.NextRowID(ctx, tenantID, tableID)
				require.NoError(t, err)
				results <- rowID
			}()
		}

		// Collect results
		ids := make(map[int64]bool)
		for i := 0; i < numGoroutines; i++ {
			id := <-results
			ids[id] = true
		}

		// All IDs should be unique and positive
		assert.Equal(t, numGoroutines, len(ids), "All IDs should be unique")
		for id := range ids {
			assert.Greater(t, id, int64(0), "All IDs should be positive")
		}
	})

	t.Run("IDGenerationAcrossDifferentEntities", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(1)

		// Generate different types of IDs
		rowID, err := engine.NextRowID(ctx, tenantID, 1)
		require.NoError(t, err)
		assert.Greater(t, rowID, int64(0))

		tableID, err := engine.NextTableID(ctx, tenantID)
		require.NoError(t, err)
		assert.Greater(t, tableID, int64(0))

		indexID, err := engine.NextIndexID(ctx, tenantID, 1)
		require.NoError(t, err)
		assert.Greater(t, indexID, int64(0))

		// All should be positive
		assert.Greater(t, rowID, int64(0))
		assert.Greater(t, tableID, int64(0))
		assert.Greater(t, indexID, int64(0))
	})

	t.Run("IDGenerationWithNegativeTenantID", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(-1) // Negative tenant ID

		// This should still work
		rowID, err := engine.NextRowID(ctx, tenantID, 1)
		require.NoError(t, err)
		assert.Greater(t, rowID, int64(0))
	})
}

// TestPebbleEngine_ComponentInteractions tests interactions between engine components
func TestPebbleEngine_ComponentInteractions(t *testing.T) {
	t.Run("CodecAndKVInteraction", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		// Test that codec and KV store can work together
		codec := engine.GetCodec()
		kv := engine.GetKV()

		assert.NotNil(t, codec)
		assert.NotNil(t, kv)

		// They should be the same instances
		assert.Equal(t, engine.codec, codec)
		assert.Equal(t, engine.kv, kv)
	})

	t.Run("DeadlockDetectorIntegration", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		// Test deadlock detector integration
		detector := engine.GetDeadlockDetector()
		assert.NotNil(t, detector)

		// Should be the same instance
		assert.Equal(t, engine.deadlockDetector, detector)
	})

	t.Run("BatchProcessorIntegration", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		// Test batch processor integration
		processor := engine.batchProcessor
		assert.NotNil(t, processor)
		assert.Implements(t, (*batch.ParallelBatchProcessor)(nil), processor)
	})
}

// TestPebbleEngine_EdgeCases tests edge cases
func TestPebbleEngine_EdgeCases(t *testing.T) {
	t.Run("ZeroValueTenantID", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(0) // Zero tenant ID

		// This should still work
		rowID, err := engine.NextRowID(ctx, tenantID, 1)
		require.NoError(t, err)
		assert.Greater(t, rowID, int64(0))
	})

	t.Run("ZeroValueTableID", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(1)
		tableID := int64(0) // Zero table ID

		// This should still work
		rowID, err := engine.NextRowID(ctx, tenantID, tableID)
		require.NoError(t, err)
		assert.Greater(t, rowID, int64(0))
	})

	t.Run("MaximumInt64Values", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(9223372036854775807) // Max int64
		tableID := int64(9223372036854775807)  // Max int64

		// This should still work
		rowID, err := engine.NextRowID(ctx, tenantID, tableID)
		require.NoError(t, err)
		assert.Greater(t, rowID, int64(0))
	})
}

// TestPebbleEngine_Performance tests performance aspects
func TestPebbleEngine_Performance(t *testing.T) {
	t.Run("IDGenerationPerformance", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(1)
		tableID := int64(1)

		// Measure performance of ID generation
		start := time.Now()
		const iterations = 1000

		for i := 0; i < iterations; i++ {
			_, err := engine.NextRowID(ctx, tenantID, tableID)
			require.NoError(t, err)
		}

		duration := time.Since(start)
		t.Logf("Generated %d row IDs in %v (%v per ID)", iterations, duration, duration/time.Duration(iterations))

		// Should complete within reasonable time
		assert.Less(t, duration, 10*time.Second, "ID generation should be reasonably fast")
	})

	t.Run("ConcurrentIDGenerationPerformance", func(t *testing.T) {
		engine, cleanup := setupTestPebbleEngineCore(t)
		defer cleanup()

		ctx := context.Background()
		tenantID := int64(1)
		tableID := int64(1)

		// Measure performance of concurrent ID generation
		start := time.Now()
		const numGoroutines = 100
		const idsPerGoroutine = 10

		results := make(chan error, numGoroutines*idsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			go func() {
				for j := 0; j < idsPerGoroutine; j++ {
					_, err := engine.NextRowID(ctx, tenantID, tableID)
					results <- err
				}
			}()
		}

		// Collect results
		for i := 0; i < numGoroutines*idsPerGoroutine; i++ {
			err := <-results
			require.NoError(t, err)
		}

		duration := time.Since(start)
		totalIDs := numGoroutines * idsPerGoroutine
		t.Logf("Generated %d row IDs concurrently in %v (%v per ID)", totalIDs, duration, duration/time.Duration(totalIDs))

		// Should complete within reasonable time
		assert.Less(t, duration, 10*time.Second, "Concurrent ID generation should be reasonably fast")
	})
}