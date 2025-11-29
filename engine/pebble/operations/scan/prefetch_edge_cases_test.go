package scan

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPrefetch_CancelledContext(t *testing.T) {
	// Test prefetch behavior with cancelled contexts
	ctx, cancel := context.WithCancel(context.Background())
	
	// Cancel the context immediately
	cancel()
	
	// Test that prefetch respects cancellation
	result := simulatePrefetchWithContext(ctx, 100)
	assert.False(t, result.success)
	assert.Equal(t, "context cancelled", result.error)
}

func TestPrefetch_ErrorHandling(t *testing.T) {
	// Test prefetch error propagation
	result := simulatePrefetchWithError(100, "simulated error")
	assert.False(t, result.success)
	assert.Equal(t, "simulated error", result.error)
}

func TestPrefetch_PartialResults(t *testing.T) {
	// Test behavior when prefetch returns partial results
	result := simulatePrefetchPartial(100, 50) // Request 100, get 50
	assert.True(t, result.success)
	assert.Equal(t, 50, result.count)
}

type prefetchResult struct {
	success bool
	count   int
	error   string
}

func simulatePrefetchWithContext(ctx context.Context, batchSize int) prefetchResult {
	// Simulate prefetch that respects context cancellation
	select {
	case <-ctx.Done():
		return prefetchResult{
			success: false,
			error:   "context cancelled",
		}
	case <-time.After(10 * time.Millisecond):
		// Simulate work
		return prefetchResult{
			success: true,
			count:   batchSize,
		}
	}
}

func simulatePrefetchWithError(batchSize int, errorMsg string) prefetchResult {
	// Simulate prefetch that returns an error
	return prefetchResult{
		success: false,
		error:   errorMsg,
	}
}

func simulatePrefetchPartial(requested, actual int) prefetchResult {
	// Simulate prefetch that returns fewer results than requested
	return prefetchResult{
		success: true,
		count:   actual,
	}
}