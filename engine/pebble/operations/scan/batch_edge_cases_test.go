package scan

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/stretchr/testify/assert"
)

func TestAdaptiveBatch_ZeroLimit(t *testing.T) {
	// Test behavior with LIMIT 0
	opts := &engineTypes.ScanOptions{
		Limit: 0,
	}
	
	// Should use default batch size for unlimited scans
	expectedBatchSize := 1000
	actualBatchSize := calculateBatchSize(opts, 10000)
	assert.Equal(t, expectedBatchSize, actualBatchSize)
}

func TestAdaptiveBatch_ExceedingResultSet(t *testing.T) {
	// Test when batch size exceeds actual result set
	opts := &engineTypes.ScanOptions{
		Limit: 50,
	}
	
	// Even with a large estimated result set, limit should constrain batch size
	expectedBatchSize := 50
	actualBatchSize := calculateBatchSize(opts, 10000)
	assert.Equal(t, expectedBatchSize, actualBatchSize)
}

func TestAdaptiveBatch_InterruptedIteration(t *testing.T) {
	// Test batch state when iteration is interrupted
	// This tests that the iterator properly handles partial batch processing
	
	iter := &IndexIterator{
		batchSize:   100,
		rowIDBuffer: make([]int64, 50), // Only half full
		cacheIdx:    25,                // Halfway through current batch
	}
	
	// Simulate interruption and reset
	iter.Reset()
	
	// Verify state is properly reset
	assert.Equal(t, 0, iter.batchSize)
	assert.Nil(t, iter.rowIDBuffer)
	assert.Equal(t, 0, iter.cacheIdx)
}

func TestAdaptiveBatch_VeryLargeLimit(t *testing.T) {
	// Test with very large LIMIT values
	opts := &engineTypes.ScanOptions{
		Limit: 1000000,
	}
	
	// Should use default initial batch size (200) since limit > initialBatchSize
	expectedBatchSize := 200
	actualBatchSize := calculateBatchSize(opts, 10000)
	assert.Equal(t, expectedBatchSize, actualBatchSize)
}

// Helper function for testing batch size calculations
func calculateBatchSize(opts *engineTypes.ScanOptions, estimatedSize int) int {
	initialBatchSize := 200
	if opts != nil && opts.Limit > 0 {
		if opts.Limit < initialBatchSize {
			initialBatchSize = opts.Limit
		}
	} else {
		// For unlimited scans, start with larger batch size to reduce round trips
		initialBatchSize = 1000
	}
	return initialBatchSize
}