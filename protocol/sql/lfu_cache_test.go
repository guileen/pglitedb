package sql

import (
	"testing"
	"time"
)

func TestLFUCacheBasicOperations(t *testing.T) {
	cache := NewLFUCache(3)
	
	// Test Put and Get
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")
	
	val, ok := cache.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Expected value1, got %v", val)
	}
	
	val, ok = cache.Get("key2")
	if !ok || val != "value2" {
		t.Errorf("Expected value2, got %v", val)
	}
	
	// Test Len
	if cache.Len() != 3 {
		t.Errorf("Expected length 3, got %d", cache.Len())
	}
	
	// Access key1 again to increase its frequency
	cache.Get("key1")
	cache.Get("key1")
	
	// Add a new item, should evict least frequently used (key2 or key3)
	cache.Put("key4", "value4")
	
	// key1 should still be in cache because it has higher frequency
	_, ok = cache.Get("key1")
	if !ok {
		t.Error("key1 should still be in cache")
	}
	
	// One of key2 or key3 should be evicted
	_, key2Exists := cache.Get("key2")
	_, key3Exists := cache.Get("key3")
	if key2Exists && key3Exists {
		t.Error("One of key2 or key3 should be evicted")
	}
}

func TestLFUCacheExpiration(t *testing.T) {
	cache := NewLFUCacheWithExpiration(3, 10*time.Millisecond)
	
	cache.Put("key1", "value1")
	
	// Should be able to get immediately
	val, ok := cache.Get("key1")
	if !ok || val != "value1" {
		t.Errorf("Expected value1, got %v", val)
	}
	
	// Wait for expiration
	time.Sleep(15 * time.Millisecond)
	
	// Should not be able to get after expiration
	_, ok = cache.Get("key1")
	if ok {
		t.Error("Key should have expired")
	}
}

func TestLFUCacheStats(t *testing.T) {
	cache := NewLFUCache(3)
	
	// Test initial stats
	hits, misses := cache.Stats()
	if hits != 0 || misses != 0 {
		t.Errorf("Expected 0 hits and 0 misses, got %d hits and %d misses", hits, misses)
	}
	
	// Test hit rate
	hitRate := cache.HitRate()
	if hitRate != 0.0 {
		t.Errorf("Expected 0.0 hit rate, got %f", hitRate)
	}
	
	// Add some items
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	
	// Get existing items (hits)
	cache.Get("key1")
	cache.Get("key2")
	
	// Get non-existing item (miss)
	cache.Get("key3")
	
	// Check stats
	hits, misses = cache.Stats()
	if hits != 2 || misses != 1 {
		t.Errorf("Expected 2 hits and 1 miss, got %d hits and %d misses", hits, misses)
	}
	
	// Check hit rate
	hitRate = cache.HitRate()
	expectedRate := float64(2) / float64(3) * 100
	if hitRate != expectedRate {
		t.Errorf("Expected %f hit rate, got %f", expectedRate, hitRate)
	}
}