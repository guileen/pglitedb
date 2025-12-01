package sql

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func BenchmarkLRUCache_Get(b *testing.B) {
	// Create cache with realistic capacity
	cache := NewLRUCache(10000)
	defer cache.Close()
	
	// Pre-populate with test data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		cache.Put(key, value)
	}
	
	b.ResetTimer()
	
	// Concurrent benchmark
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i%1000)
			_, found := cache.Get(key)
			if !found {
				b.Fatalf("Key not found: %s", key)
			}
			i++
		}
	})
}

func BenchmarkLRUCache_Put(b *testing.B) {
	// Create cache with realistic capacity
	cache := NewLRUCache(10000)
	defer cache.Close()
	
	b.ResetTimer()
	
	// Concurrent benchmark
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			value := fmt.Sprintf("value_%d", i)
			cache.Put(key, value)
			i++
		}
	})
}

func BenchmarkLRUCache_MixedWorkload(b *testing.B) {
	// Create cache with realistic capacity
	cache := NewLRUCache(10000)
	defer cache.Close()
	
	// Pre-populate with some data
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("existing_key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		cache.Put(key, value)
	}
	
	b.ResetTimer()
	
	// Mixed workload: 70% reads, 30% writes
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 < 7 {
				// Read operation (70%)
				key := fmt.Sprintf("existing_key_%d", i%100)
				cache.Get(key)
			} else {
				// Write operation (30%)
				key := fmt.Sprintf("new_key_%d", i)
				value := fmt.Sprintf("value_%d", i)
				cache.Put(key, value)
			}
			i++
		}
	})
}

func BenchmarkLRUCache_WithExpiration(b *testing.B) {
	// Create cache with expiration
	cache := NewLRUCacheWithExpiration(10000, 5*time.Minute)
	defer cache.Close()
	
	b.ResetTimer()
	
	// Mixed workload with expiration
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key_%d", i)
			value := fmt.Sprintf("value_%d", i)
			
			// Put with expiration
			cache.PutWithExpiration(key, value, 10*time.Minute)
			
			// Get the value back
			_, found := cache.Get(key)
			if !found {
				b.Fatalf("Key not found: %s", key)
			}
			
			i++
		}
	})
}

func BenchmarkLRUCache_ConcurrentStress(b *testing.B) {
	// Create cache with realistic capacity
	cache := NewLRUCache(50000)
	defer cache.Close()
	
	// Pre-populate with test data
	const initialSize = 10000
	for i := 0; i < initialSize; i++ {
		key := fmt.Sprintf("pre_key_%d", i)
		value := fmt.Sprintf("pre_value_%d", i)
		cache.Put(key, value)
	}
	
	b.ResetTimer()
	
	// High concurrency stress test
	var wg sync.WaitGroup
	numGoroutines := 50
	opsPerGoroutine := b.N / numGoroutines
	
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for i := 0; i < opsPerGoroutine; i++ {
				// Alternate between existing and new keys
				if i%3 == 0 {
					// Read existing key
					key := fmt.Sprintf("pre_key_%d", i%initialSize)
					cache.Get(key)
				} else {
					// Write new key
					key := fmt.Sprintf("new_key_%d_%d", goroutineID, i)
					value := fmt.Sprintf("new_value_%d_%d", goroutineID, i)
					cache.Put(key, value)
				}
			}
		}(g)
	}
	
	wg.Wait()
}