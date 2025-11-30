package batch

import (
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
)

func BenchmarkParallelBatchProcessor_ChunkSizeCalculation(b *testing.B) {
	// Create a test KV store
	kvConfig := storage.TestOptimizedPebbleConfig(b.TempDir())
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		b.Fatalf("Failed to create KV store: %v", err)
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

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Test various batch sizes
		batchSizes := []int{50, 150, 1000, 5000}
		for _, size := range batchSizes {
			_ = pbp.getOptimalChunkSize(size)
		}
	}
}