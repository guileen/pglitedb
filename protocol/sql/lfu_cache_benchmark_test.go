package sql

import (
	"strconv"
	"testing"
)

func BenchmarkLFUCache(b *testing.B) {
	cache := NewLFUCache(1000)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%500)
		if i%3 == 0 {
			cache.Put(key, "value_"+strconv.Itoa(i))
		} else {
			cache.Get(key)
		}
	}
}

func BenchmarkLRUCache(b *testing.B) {
	cache := NewLRUCache(1000)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i%500)
		if i%3 == 0 {
			cache.Put(key, "value_"+strconv.Itoa(i))
		} else {
			cache.Get(key)
		}
	}
}

func BenchmarkLFUCacheVsLRU(b *testing.B) {
	// Compare LFU and LRU cache performance with mixed workload
	keys := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		keys[i] = "key" + strconv.Itoa(i)
	}
	
	b.Run("LFU", func(b *testing.B) {
		cache := NewLFUCache(500)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%1000]
			if i%3 == 0 {
				cache.Put(key, "value_"+strconv.Itoa(i))
			} else {
				cache.Get(key)
			}
		}
	})
	
	b.Run("LRU", func(b *testing.B) {
		cache := NewLRUCache(500)
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			key := keys[i%1000]
			if i%3 == 0 {
				cache.Put(key, "value_"+strconv.Itoa(i))
			} else {
				cache.Get(key)
			}
		}
	})
}