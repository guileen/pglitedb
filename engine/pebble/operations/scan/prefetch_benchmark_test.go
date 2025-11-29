package scan

import (
	"context"
	"sync"
	"testing"
)

func BenchmarkPrefetch_SingleThread(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate single-threaded prefetching
		simulatePrefetchWork(1000, 1)
	}
}

func BenchmarkPrefetch_MultiThread(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		// Simulate multi-threaded prefetching
		simulatePrefetchWork(1000, 4)
	}
}

func BenchmarkPrefetch_VaryingBatchSizes(b *testing.B) {
	batchSizes := []int{10, 50, 100, 500, 1000}
	
	for _, batchSize := range batchSizes {
		b.Run("Batch"+string(rune(batchSize)), func(b *testing.B) {
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				simulatePrefetchWork(batchSize, 2)
			}
		})
	}
}

func simulatePrefetchWork(batchSize, concurrency int) {
	var wg sync.WaitGroup
	workChan := make(chan int, concurrency)
	
	// Start worker goroutines
	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range workChan {
				// Simulate prefetch work
				prefetchBatch(batchSize)
			}
		}()
	}
	
	// Send work to workers
	totalWork := 10000
	for i := 0; i < totalWork; i++ {
		workChan <- i
	}
	close(workChan)
	
	wg.Wait()
}

func prefetchBatch(size int) {
	ctx := context.Background()
	
	// Simulate async prefetch operation
	done := make(chan struct{})
	go func() {
		// Simulate I/O work
		_ = make([]byte, size*100)
		close(done)
	}()
	
	select {
	case <-done:
		// Prefetch completed
	case <-ctx.Done():
		// Context cancelled
	}
}