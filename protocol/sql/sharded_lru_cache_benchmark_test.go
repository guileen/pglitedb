package sql

import (
	"strconv"
	"testing"
)

func BenchmarkShardedLRUCacheGet(b *testing.B) {
	cache := NewShardedLRUCache(1000)

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

func BenchmarkShardedLRUCachePut(b *testing.B) {
	cache := NewShardedLRUCache(1000)

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

func BenchmarkShardedLRUCacheMixed(b *testing.B) {
	cache := NewShardedLRUCache(1000)

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

// Comparison benchmarks with regular LRU cache
func BenchmarkRegularVsShardedGet(b *testing.B) {
	b.Run("RegularLRU", func(b *testing.B) {
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
	})

	b.Run("ShardedLRU", func(b *testing.B) {
		cache := NewShardedLRUCache(1000)

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
	})
}

func BenchmarkRegularVsShardedPut(b *testing.B) {
	b.Run("RegularLRU", func(b *testing.B) {
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
	})

	b.Run("ShardedLRU", func(b *testing.B) {
		cache := NewShardedLRUCache(1000)

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				key := "key" + strconv.Itoa(i)
				cache.Put(key, "value"+strconv.Itoa(i))
				i++
			}
		})
	})
}

func BenchmarkRegularVsShardedMixed(b *testing.B) {
	b.Run("RegularLRU", func(b *testing.B) {
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
	})

	b.Run("ShardedLRU", func(b *testing.B) {
		cache := NewShardedLRUCache(1000)

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
	})
}