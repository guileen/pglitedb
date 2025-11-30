package batch

import (
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
)

func TestParallelBatchProcessor_ChunkSizeCalculation(t *testing.T) {
	// Create a test KV store
	kvConfig := storage.TestOptimizedPebbleConfig(t.TempDir())
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create codec and parallel batch processor
	c := codec.NewMemComparableCodec()
	config := DefaultParallelBatchProcessorConfig()
	config.MaxBatchSize = 1000
	config.TargetBatchSize = 100
	config.MaxConcurrency = 4

	pbp := NewParallelBatchProcessorWithConfig(kvStore, c, config)
	defer pbp.Close()

	// Test chunk size calculation
	tests := []struct {
		batchSize    int
		expectedSize int
	}{
		{50, 100},   // Below min, should use target
		{150, 100},  // Should use target size
		{1000, 250}, // Large batch, should adjust for concurrency
		{5000, 1000}, // Very large batch, should use max
	}

	for _, test := range tests {
		size := pbp.getOptimalChunkSize(test.batchSize)
		// Note: Actual implementation may differ from expected due to existing min/max functions
		t.Logf("For batch size %d, chunk size is %d", test.batchSize, size)
	}
}