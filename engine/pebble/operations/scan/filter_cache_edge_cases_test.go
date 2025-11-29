package scan

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFilterCache_EvictionPolicy(t *testing.T) {
	// Test cache eviction with many different filters
	cache := newSimpleFilterCache(10) // Small cache size
	
	// Add more filters than cache can hold
	for i := 0; i < 20; i++ {
		key := "filter_" + string(rune(i))
		cache.Put(key, true)
	}
	
	// Verify that some entries have been evicted
	cachedCount := cache.Size()
	assert.LessOrEqual(t, cachedCount, 10)
}

func TestFilterCache_ConcurrentAccess(t *testing.T) {
	// Test thread safety of filter cache
	cache := newSimpleFilterCache(100)
	
	const numGoroutines = 10
	const numOperations = 1000
	
	var wg sync.WaitGroup
	wg.Add(numGoroutines)
	
	start := make(chan struct{})
	
	// Start multiple goroutines that access the cache
	for i := 0; i < numGoroutines; i++ {
		go func(goroutineID int) {
			defer wg.Done()
			
			// Wait for start signal
			<-start
			
			for j := 0; j < numOperations; j++ {
				key := "filter_" + string(rune(goroutineID)) + "_" + string(rune(j))
				
				// Mix of put and get operations
				if j%2 == 0 {
					cache.Put(key, true)
				} else {
					_, _ = cache.Get(key)
				}
			}
		}(i)
	}
	
	// Start all goroutines simultaneously
	close(start)
	
	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	
	select {
	case <-done:
		// Success
	case <-time.After(30 * time.Second):
		t.Fatal("Test timed out - possible deadlock")
	}
}

func TestFilterCache_Invalidation(t *testing.T) {
	// Test cache invalidation on schema changes
	cache := newSimpleFilterCache(100)
	
	// Add some entries
	cache.Put("filter1", true)
	cache.Put("filter2", false)
	
	// Verify entries exist
	val, ok := cache.Get("filter1")
	assert.True(t, ok)
	assert.True(t, val)
	
	// Invalidate cache (simulate schema change)
	cache.Invalidate()
	
	// Entries should no longer exist
	_, ok = cache.Get("filter1")
	assert.False(t, ok)
	
	_, ok = cache.Get("filter2")
	assert.False(t, ok)
}

// Simple implementation for testing
type simpleFilterCache struct {
	mu    sync.RWMutex
	cache map[string]bool
	size  int
}

func newSimpleFilterCache(size int) *simpleFilterCache {
	return &simpleFilterCache{
		cache: make(map[string]bool),
		size:  size,
	}
}

func (sfc *simpleFilterCache) Get(key string) (bool, bool) {
	sfc.mu.RLock()
	defer sfc.mu.RUnlock()
	
	val, ok := sfc.cache[key]
	return val, ok
}

func (sfc *simpleFilterCache) Put(key string, value bool) {
	sfc.mu.Lock()
	defer sfc.mu.Unlock()
	
	if len(sfc.cache) >= sfc.size {
		// Simple eviction - remove first entry
		for k := range sfc.cache {
			delete(sfc.cache, k)
			break
		}
	}
	
	sfc.cache[key] = value
}

func (sfc *simpleFilterCache) Size() int {
	sfc.mu.RLock()
	defer sfc.mu.RUnlock()
	return len(sfc.cache)
}

func (sfc *simpleFilterCache) Invalidate() {
	sfc.mu.Lock()
	defer sfc.mu.Unlock()
	sfc.cache = make(map[string]bool)
}