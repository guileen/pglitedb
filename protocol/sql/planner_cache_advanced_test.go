package sql

import (
	"testing"
	"time"
)

func TestPlannerCacheWithExpiration(t *testing.T) {
	// Create a planner with simple parser and cache with expiration
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	// Override the cache with one that has expiration
	planner.planCache = NewLRUCacheWithExpiration(100, 100*time.Millisecond) // 100ms expiration
	
	// Reset stats to start fresh
	planner.ResetCacheStats()

	// Test query
	query := "SELECT * FROM users WHERE id = 1"

	// First plan creation (cache miss)
	plan1, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create first plan: %v", err)
	}

	// Should have 0 hits, 1 miss
	hits, misses := planner.CacheStats()
	if hits != 0 || misses != 1 {
		t.Errorf("After first plan: hits=%d, misses=%d", hits, misses)
	}

	// Second plan creation (cache hit)
	plan2, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create second plan: %v", err)
	}

	// Should have 1 hit, 1 miss
	hits, misses = planner.CacheStats()
	if hits != 1 || misses != 1 {
		t.Errorf("After second plan: hits=%d, misses=%d", hits, misses)
	}

	// Plans should be equivalent
	if plan1.QueryString != plan2.QueryString {
		t.Errorf("Cached plans should be equivalent")
	}

	// Wait for cache entry to expire
	time.Sleep(150 * time.Millisecond)

	// Third plan creation (cache miss due to expiration)
	plan3, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create third plan: %v", err)
	}

	// Should have 1 hit, 2 misses
	hits, misses = planner.CacheStats()
	if hits != 1 || misses != 2 {
		t.Errorf("After third plan: hits=%d, misses=%d", hits, misses)
	}

	// Plans should still be equivalent
	if plan1.QueryString != plan3.QueryString {
		t.Errorf("Plans should still be equivalent after expiration")
	}
}

func TestPlannerCacheEviction(t *testing.T) {
	// Create a planner with small cache capacity
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	// Override the cache with a smaller one
	planner.planCache = NewLRUCache(2) // Only 2 entries capacity
	
	// Reset stats to start fresh
	planner.ResetCacheStats()

	// Add 3 different queries to trigger eviction
	query1 := "SELECT * FROM users WHERE id = 1"
	query2 := "SELECT * FROM users WHERE id = 2"
	query3 := "SELECT * FROM users WHERE id = 3"

	// Create plans for all queries
	_, err := planner.CreatePlan(query1)
	if err != nil {
		t.Fatalf("Failed to create plan 1: %v", err)
	}
	
	_, err = planner.CreatePlan(query2)
	if err != nil {
		t.Fatalf("Failed to create plan 2: %v", err)
	}
	
	_, err = planner.CreatePlan(query3)
	if err != nil {
		t.Fatalf("Failed to create plan 3: %v", err)
	}

	// Cache should have 2 entries (capacity limit)
	if size := planner.PlanCacheSize(); size != 2 {
		t.Errorf("Cache size should be 2, got %d", size)
	}

	// Should have 0 hits, 3 misses
	hits, misses := planner.CacheStats()
	if hits != 0 || misses != 3 {
		t.Errorf("Stats should be 0 hits, 3 misses: hits=%d, misses=%d", hits, misses)
	}

	// Now access query2 and query3 to make them recently used
	_, err = planner.CreatePlan(query2)
	if err != nil {
		t.Fatalf("Failed to recreate plan 2: %v", err)
	}
	
	_, err = planner.CreatePlan(query3)
	if err != nil {
		t.Fatalf("Failed to recreate plan 3: %v", err)
	}

	// Should have 2 hits, 3 misses now
	hits, misses = planner.CacheStats()
	if hits != 2 || misses != 3 {
		t.Errorf("Stats should be 2 hits, 3 misses: hits=%d, misses=%d", hits, misses)
	}

	// Now access query1 again - it should be a miss because it was evicted
	_, err = planner.CreatePlan(query1)
	if err != nil {
		t.Fatalf("Failed to recreate plan 1: %v", err)
	}

	// Should have 2 hits, 4 misses
	hits, misses = planner.CacheStats()
	if hits != 2 || misses != 4 {
		t.Errorf("Stats should be 2 hits, 4 misses: hits=%d, misses=%d", hits, misses)
	}
}