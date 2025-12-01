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

// Benchmark for optimized chunk size calculation with realistic workloads
func BenchmarkParallelBatchProcessor_OptimizedChunkSize(b *testing.B) {
	// Create a test KV store
	kvConfig := storage.TestOptimizedPebbleConfig(b.TempDir())
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		b.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create codec and parallel batch processor with optimized config
	c := codec.NewMemComparableCodec()
	config := DefaultParallelBatchProcessorConfig()
	config.MaxBatchSize = 100000
	config.TargetBatchSize = 10000
	config.MaxConcurrency = 16

	pbp := NewParallelBatchProcessorWithConfig(kvStore, c, config)
	defer pbp.Close()

	// Realistic batch sizes from production workloads
	realisticBatchSizes := []int{1000, 5000, 10000, 25000, 50000, 100000, 200000}

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		batchSize := realisticBatchSizes[i%len(realisticBatchSizes)]
		_ = pbp.getOptimalChunkSize(batchSize)
	}
}

// Benchmark for parallel processing with optimized configuration
func BenchmarkParallelBatchProcessor_ProcessingThroughput(b *testing.B) {
	// Create a test KV store with high-performance config
	kvConfig := storage.HighPerformancePebbleConfig(b.TempDir())
	kvStore, err := storage.NewPebbleKV(kvConfig)
	if err != nil {
		b.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create codec and parallel batch processor with optimized config
	c := codec.NewMemComparableCodec()
	config := DefaultParallelBatchProcessorConfig()
	
	pbp := NewParallelBatchProcessorWithConfig(kvStore, c, config)
	defer pbp.Close()

	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate processing with different batch sizes
		batchSizes := []int{1000, 10000, 50000}
		batchSize := batchSizes[i%len(batchSizes)]
		
		// This is a simplified benchmark - in real usage, this would process actual data
		chunkSize := pbp.getOptimalChunkSize(batchSize)
		concurrency := min(config.MaxConcurrency, (batchSize+chunkSize-1)/chunkSize)
		
		// Simulate the work distribution calculation
		_ = concurrency
		_ = chunkSize
	}
}