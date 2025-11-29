package sql

import (
	"testing"
)

func TestPlannerPlanCaching(t *testing.T) {
	// Create a planner with simple parser
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)

	// Test query
	query := "SELECT * FROM users WHERE id = 1"

	// First plan creation
	plan1, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create first plan: %v", err)
	}

	// Second plan creation (should use cache)
	plan2, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create second plan: %v", err)
	}

	// Verify both plans are equivalent
	if plan1.QueryString != plan2.QueryString {
		t.Errorf("Cached plan query string mismatch: %s != %s", plan1.QueryString, plan2.QueryString)
	}

	if plan1.Type != plan2.Type {
		t.Errorf("Cached plan type mismatch: %v != %v", plan1.Type, plan2.Type)
	}

	// Verify cache is populated
	if planner.PlanCacheSize() != 1 {
		t.Errorf("Expected cache size 1, got %d", planner.PlanCacheSize())
	}

	// Clear cache
	planner.ClearPlanCache()
	if planner.PlanCacheSize() != 0 {
		t.Errorf("Expected cache size 0 after clear, got %d", planner.PlanCacheSize())
	}

	// Test with caching disabled
	planner.EnablePlanCaching(false)
	plan3, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create plan with caching disabled: %v", err)
	}

	if planner.PlanCacheSize() != 0 {
		t.Errorf("Expected cache size 0 with caching disabled, got %d", planner.PlanCacheSize())
	}

	// Re-enable caching
	planner.EnablePlanCaching(true)
	plan4, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create plan with caching re-enabled: %v", err)
	}

	if planner.PlanCacheSize() != 1 {
		t.Errorf("Expected cache size 1 after re-enabling, got %d", planner.PlanCacheSize())
	}

	// Verify plan integrity
	if plan3.QueryString != plan4.QueryString {
		t.Errorf("Plan query string mismatch after cache toggle: %s != %s", plan3.QueryString, plan4.QueryString)
	}
}

func TestLRUCache(t *testing.T) {
	// Test basic LRU cache functionality
	cache := NewLRUCache(3)

	// Add items
	cache.Put("key1", "value1")
	cache.Put("key2", "value2")
	cache.Put("key3", "value3")

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

	// Add fourth item, should evict key1 (least recently used)
	cache.Put("key4", "value4")

	// key1 should be evicted
	if _, ok := cache.Get("key1"); ok {
		t.Errorf("key1 should have been evicted")
	}

	// Other keys should still be present
	if val, ok := cache.Get("key2"); !ok || val != "value2" {
		t.Errorf("key2 should still be present")
	}

	if val, ok := cache.Get("key3"); !ok || val != "value3" {
		t.Errorf("key3 should still be present")
	}

	if val, ok := cache.Get("key4"); !ok || val != "value4" {
		t.Errorf("key4 should be present")
	}

	// Access key2 to make it most recently used
	cache.Get("key2")

	// Add fifth item, should evict key3 (now least recently used)
	cache.Put("key5", "value5")

	// key3 should be evicted
	if _, ok := cache.Get("key3"); ok {
		t.Errorf("key3 should have been evicted")
	}

	// key2 should still be present (it was recently accessed)
	if val, ok := cache.Get("key2"); !ok || val != "value2" {
		t.Errorf("key2 should still be present")
	}
}