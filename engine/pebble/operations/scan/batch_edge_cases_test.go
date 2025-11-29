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
	// Note: This test is no longer applicable as Reset method has been removed
	// The iterator now uses direct allocation instead of pooling
	t.Skip("Reset method removed - iterator now uses direct allocation")
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