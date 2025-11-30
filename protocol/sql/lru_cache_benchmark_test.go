package sql

import (
	"strconv"
	"testing"
	"time"
)

func BenchmarkLRUCacheGet(b *testing.B) {
	cache := NewLRUCache(1000)
	
	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Put("key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i%100)
			cache.Get(key)
			i++
		}
	})
}

func BenchmarkLRUCachePut(b *testing.B) {
	cache := NewLRUCache(1000)
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + strconv.Itoa(i)
			cache.Put(key, "value"+strconv.Itoa(i))
			i++
		}
	})
}

func BenchmarkLRUCacheMixed(b *testing.B) {
	cache := NewLRUCache(1000)
	
	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Put("key"+strconv.Itoa(i), "value"+strconv.Itoa(i))
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%2 == 0 {
				// Get operation
				key := "key" + strconv.Itoa(i%100)
				cache.Get(key)
			} else {
				// Put operation
				key := "key" + strconv.Itoa(i)
				cache.Put(key, "value"+strconv.Itoa(i))
			}
			i++
		}
	})
}