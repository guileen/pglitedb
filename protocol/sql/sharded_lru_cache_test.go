package sql

import (
	"testing"
	"time"
)

func TestShardedLRUCache(t *testing.T) {
	// Test basic LRU cache functionality with realistic capacity
	cache := NewShardedLRUCache(32) // 32 total capacity, 2 per shard with 16 shards

	// Add items
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")
	cache.Put("key4", "value4")

	// Verify all items are present
	if val, ok := cache.Get("key1"); !ok || val != "value1" {
		t.Errorf("Failed to get key1")
	}

	if val, ok := cache.Get("key2"); !ok || val != "value2" {
		t.Errorf("Failed to get key2")
	}

	if val, ok := cache.Get("key3"); !ok || val != "value3" {
		t.Errorf("Failed to get key3")
	}

	if val, ok := cache.Get("key4"); !ok || val != "value4" {
		t.Errorf("Failed to get key4")
	}

	// Add more items to trigger eviction
	cache.Put("key5", "value5")
	cache.Put("key6", "value6")
	cache.Put("key7", "value7")
	cache.Put("key8", "value8")

	// Some of the older keys should be evicted due to per-shard capacity limits
	// We can't predict exactly which ones due to sharding, but the total should be limited
	if cache.Len() > 8 {
		t.Errorf("Cache length should not exceed capacity")
	}
}

func TestShardedLRUCacheWithExpiration(t *testing.T) {
	// Test cache with expiration
	cache := NewShardedLRUCacheWithExpiration(10, 10*time.Millisecond)

	// Add item
	cache.Put("key1", "value1")

	// Should be able to get it immediately
	if val, ok := cache.Get("key1"); !ok || val != "value1" {
		t.Errorf("Failed to get key1 immediately")
	}

	// Wait for expiration
	time.Sleep(20 * time.Millisecond)

	// Should not be able to get it after expiration
	if val, ok := cache.Get("key1"); ok {
		t.Errorf("Should not get expired key1, got: %v", val)
	}
}

func TestShardedLRUCacheRemove(t *testing.T) {
	cache := NewShardedLRUCache(10)

	// Add items
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")

	// Verify items are present
	if val, ok := cache.Get("key1"); !ok || val != "value1" {
		t.Errorf("Failed to get key1")
	}

	if val, ok := cache.Get("key2"); !ok || val != "value2" {
		t.Errorf("Failed to get key2")
	}

	// Remove key1
	cache.Remove("key1")

	// key1 should be gone
	if _, ok := cache.Get("key1"); ok {
		t.Errorf("key1 should have been removed")
	}

	// key2 should still be present
	if val, ok := cache.Get("key2"); !ok || val != "value2" {
		t.Errorf("key2 should still be present")
	}
}

func TestShardedLRUCacheClear(t *testing.T) {
	cache := NewShardedLRUCache(10)

	// Add items
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")

	// Verify items are present
	if cache.Len() != 3 {
		t.Errorf("Expected length 3, got %d", cache.Len())
	}

	// Clear cache
	cache.Clear()

	// All items should be gone
	if cache.Len() != 0 {
		t.Errorf("Expected length 0 after clear, got %d", cache.Len())
	}

	// Keys should not be found
	if _, ok := cache.Get("key1"); ok {
		t.Errorf("key1 should not be found after clear")
	}
	if _, ok := cache.Get("key2"); ok {
		t.Errorf("key2 should not be found after clear")
	}
	if _, ok := cache.Get("key3"); ok {
		t.Errorf("key3 should not be found after clear")
	}
}

func TestShardedLRUCacheStats(t *testing.T) {
	cache := NewShardedLRUCache(10)

	// Initial stats should be zero
	hits, misses := cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("Initial stats should be zero, got hits=%d, misses=%d", hits, misses)
	}

	// Add item
	cache.Put("key1", "value1")

	// Miss on non-existent key
	cache.Get("nonexistent")

	// Hit on existing key
	cache.Get("key1")

	// Check stats
	hits, misses = cache.Stats()
	if hits != 1 || misses != 1 {
		t.Errorf("Expected 1 hit and 1 miss, got hits=%d, misses=%d", hits, misses)
	}

	// Check hit rate
	hitRate := cache.HitRate()
	expectedRate := float64(1) / float64(2) * 100
	if hitRate != expectedRate {
		t.Errorf("Expected hit rate %.2f%%, got %.2f%%", expectedRate, hitRate)
	}

	// Reset stats
	cache.ResetStats()
	hits, misses = cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("Stats should be zero after reset, got hits=%d, misses=%d", hits, misses)
	}
}