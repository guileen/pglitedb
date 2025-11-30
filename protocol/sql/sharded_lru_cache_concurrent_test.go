package sql

import (
	"strconv"
	"sync"
	"testing"
)

func TestShardedLRUCacheConcurrentAccess(t *testing.T) {
	cache := NewShardedLRUCache(1000)
	const numWorkers = 100
	const numOperations = 1000

	var wg sync.WaitGroup
	wg.Add(numWorkers * 2) // 100 put workers, 100 get workers

	// Start put workers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "worker" + strconv.Itoa(workerID) + "_key" + strconv.Itoa(j)
				value := "value" + strconv.Itoa(workerID) + "_" + strconv.Itoa(j)
				cache.Put(key, value)
			}
		}(i)
	}

	// Start get workers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for j := 0; j < numOperations; j++ {
				key := "worker" + strconv.Itoa(workerID) + "_key" + strconv.Itoa(j)
				// Try to get the value, we don't care about the result for this test
				// Just testing concurrent access doesn't cause panics
				cache.Get(key)
			}
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()

	// Verify cache is not corrupted
	if cache.Len() > 1000 {
		t.Errorf("Cache size exceeded capacity: %d", cache.Len())
	}
}

func TestShardedLRUCacheConcurrentPutSameKeys(t *testing.T) {
	cache := NewShardedLRUCache(100)
	const numWorkers = 50
	const numKeys = 10

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Multiple workers putting the same keys concurrently
	for i := 0; i < numWorkers; i++ {
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				for k := 0; k < numKeys; k++ {
					key := "key" + strconv.Itoa(k)
					value := "value" + strconv.Itoa(j)
					cache.Put(key, value)
				}
			}
		}()
	}

	wg.Wait()

	// Verify all keys are present
	for i := 0; i < numKeys; i++ {
		key := "key" + strconv.Itoa(i)
		if val, ok := cache.Get(key); !ok {
			t.Errorf("Key %s should be present", key)
		} else if val == nil {
			t.Errorf("Value for key %s should not be nil", key)
		}
	}
}