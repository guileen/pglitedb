package scan

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
)

func BenchmarkAdaptiveBatch_SmallResultSet(b *testing.B) {
	benchmarkAdaptiveBatch(b, 50) // Small result set
}

func BenchmarkAdaptiveBatch_LargeResultSet(b *testing.B) {
	benchmarkAdaptiveBatch(b, 50000) // Large result set
}

func BenchmarkAdaptiveBatch_VaryingLimits(b *testing.B) {
	// Test with different limit values
	limits := []int{10, 100, 1000, 10000}
	
	for _, limit := range limits {
		b.Run("Limit"+string(rune(limit)), func(b *testing.B) {
			benchmarkAdaptiveBatchWithLimit(b, 10000, limit)
		})
	}
}

func benchmarkAdaptiveBatch(b *testing.B, resultSetSize int) {
	b.Helper()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate adaptive batch sizing logic
		batchSize := calculateAdaptiveBatchSize(nil, resultSetSize)
		
		// Simulate processing batches
		processed := 0
		for processed < resultSetSize {
			currentBatchSize := batchSize
			if processed+currentBatchSize > resultSetSize {
				currentBatchSize = resultSetSize - processed
			}
			
			// Simulate batch processing
			processBatch(currentBatchSize)
			processed += currentBatchSize
		}
	}
}

func benchmarkAdaptiveBatchWithLimit(b *testing.B, resultSetSize, limit int) {
	b.Helper()
	
	opts := &engineTypes.ScanOptions{
		Limit: limit,
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate adaptive batch sizing logic with limit
		batchSize := calculateAdaptiveBatchSize(opts, resultSetSize)
		
		// Simulate processing batches
		processed := 0
		for processed < resultSetSize && processed < limit {
			currentBatchSize := batchSize
			if processed+currentBatchSize > resultSetSize {
				currentBatchSize = resultSetSize - processed
			}
			if processed+currentBatchSize > limit {
				currentBatchSize = limit - processed
			}
			
			// Simulate batch processing
			processBatch(currentBatchSize)
			processed += currentBatchSize
		}
	}
}

// Helper functions for benchmarking
func calculateAdaptiveBatchSize(opts *engineTypes.ScanOptions, estimatedSize int) int {
	initialBatchSize := 200
	if opts != nil && opts.Limit > 0 {
		if opts.Limit < initialBatchSize {
			initialBatchSize = opts.Limit
		}
	} else {
		if estimatedSize > 10000 {
			initialBatchSize = 1000
		} else if estimatedSize > 1000 {
			initialBatchSize = 500
		}
	}
	return initialBatchSize
}

func processBatch(size int) {
	// Simulate batch processing work
	_ = make([]byte, size*100) // Allocate some memory
}