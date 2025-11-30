package sql

import (
	"strconv"
	"sync"
	"testing"
	"time"
)

// TestCachePerformanceComparison validates the performance improvement
// of sharded LRU cache over regular LRU cache under concurrent load
func TestCachePerformanceComparison(t *testing.T) {
	const numOperations = 10000
	const numWorkers = 50
	
	// Test regular LRU cache
	t.Run("RegularLRU", func(t *testing.T) {
		cache := NewLRUCache(1000)
		var wg sync.WaitGroup
		start := time.Now()
		
		// Concurrent mixed workload
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOperations/numWorkers; j++ {
					key := "key" + strconv.Itoa(j%500)
					if j%3 == 0 {
						// Put operation
						cache.Put(key, "value_"+strconv.Itoa(workerID)+"_"+strconv.Itoa(j))
					} else {
						// Get operation
						cache.Get(key)
					}
				}
			}(i)
		}
		
		wg.Wait()
		duration := time.Since(start)
		t.Logf("Regular LRU cache took %v for %d operations", duration, numOperations)
		
		// Report stats
		hits, misses := cache.Stats()
		hitRate := cache.HitRate()
		t.Logf("Regular LRU cache stats - Hits: %d, Misses: %d, Hit Rate: %.2f%%", hits, misses, hitRate)
	})
	
	// Test sharded LRU cache
	t.Run("ShardedLRU", func(t *testing.T) {
		cache := NewShardedLRUCache(1000)
		var wg sync.WaitGroup
		start := time.Now()
		
		// Concurrent mixed workload
		for i := 0; i < numWorkers; i++ {
			wg.Add(1)
			go func(workerID int) {
				defer wg.Done()
				for j := 0; j < numOperations/numWorkers; j++ {
					key := "key" + strconv.Itoa(j%500)
					if j%3 == 0 {
						// Put operation
						cache.Put(key, "value_"+strconv.Itoa(workerID)+"_"+strconv.Itoa(j))
					} else {
						// Get operation
						cache.Get(key)
					}
				}
			}(i)
		}
		
		wg.Wait()
		duration := time.Since(start)
		t.Logf("Sharded LRU cache took %v for %d operations", duration, numOperations)
		
		// Report stats
		hits, misses := cache.Stats()
		hitRate := cache.HitRate()
		t.Logf("Sharded LRU cache stats - Hits: %d, Misses: %d, Hit Rate: %.2f%%", hits, misses, hitRate)
	})
}