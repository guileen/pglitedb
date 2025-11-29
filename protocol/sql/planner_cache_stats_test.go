package sql

import (
	"testing"
)

func TestPlannerCacheStatistics(t *testing.T) {
	// Create a planner with simple parser
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)

	// Reset stats to start fresh
	planner.ResetCacheStats()

	// Initial stats should be zero
	hits, misses := planner.CacheStats()
	if hits != 0 || misses != 0 {
		t.Errorf("Initial stats should be zero: hits=%d, misses=%d", hits, misses)
	}

	// Hit rate should be 0%
	hitRate := planner.CacheHitRate()
	if hitRate != 0.0 {
		t.Errorf("Initial hit rate should be 0%%, got %.2f%%", hitRate)
	}

	// Test query
	query := "SELECT * FROM users WHERE id = 1"

	// First plan creation (cache miss)
	_, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create first plan: %v", err)
	}

	// Should have 1 miss, 0 hits
	hits, misses = planner.CacheStats()
	if hits != 0 || misses != 1 {
		t.Errorf("After first plan: hits=%d, misses=%d", hits, misses)
	}

	// Hit rate should be 0%
	hitRate = planner.CacheHitRate()
	if hitRate != 0.0 {
		t.Errorf("Hit rate after first plan should be 0%%, got %.2f%%", hitRate)
	}

	// Second plan creation (cache hit)
	_, err = planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create second plan: %v", err)
	}

	// Should have 1 hit, 1 miss
	hits, misses = planner.CacheStats()
	if hits != 1 || misses != 1 {
		t.Errorf("After second plan: hits=%d, misses=%d", hits, misses)
	}

	// Hit rate should be 50%
	hitRate = planner.CacheHitRate()
	if hitRate != 50.0 {
		t.Errorf("Hit rate after second plan should be 50%%, got %.2f%%", hitRate)
	}

	// Test with different query (cache miss)
	differentQuery := "SELECT * FROM users WHERE id = 2"
	_, err = planner.CreatePlan(differentQuery)
	if err != nil {
		t.Fatalf("Failed to create third plan: %v", err)
	}

	// Should have 1 hit, 2 misses
	hits, misses = planner.CacheStats()
	if hits != 1 || misses != 2 {
		t.Errorf("After third plan: hits=%d, misses=%d", hits, misses)
	}

	// Hit rate should be 33.33%
	hitRate = planner.CacheHitRate()
	if hitRate < 33.32 || hitRate > 33.34 {
		t.Errorf("Hit rate after third plan should be ~33.33%%, got %.2f%%", hitRate)
	}

	// Reset stats
	planner.ResetCacheStats()
	hits, misses = planner.CacheStats()
	if hits != 0 || misses != 0 {
		t.Errorf("Stats after reset should be zero: hits=%d, misses=%d", hits, misses)
	}
	hitRate = planner.CacheHitRate()
	if hitRate != 0.0 {
		t.Errorf("Hit rate after reset should be 0%%, got %.2f%%", hitRate)
	}
}

func TestPlannerNormalizedQueryCaching(t *testing.T) {
	// Create a planner with simple parser
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)

	// Test queries with different formatting but same meaning
	query1 := "SELECT * FROM users WHERE id = 1"
	query2 := "select * from users where id = 1"  // lowercase
	query3 := "  SELECT   *   FROM   users   WHERE   id   =   1  "  // extra spaces

	// First plan creation
	plan1, err := planner.CreatePlan(query1)
	if err != nil {
		t.Fatalf("Failed to create first plan: %v", err)
	}

	// Second plan with different case (should hit cache)
	plan2, err := planner.CreatePlan(query2)
	if err != nil {
		t.Fatalf("Failed to create second plan: %v", err)
	}

	// Third plan with extra spaces (should hit cache)
	plan3, err := planner.CreatePlan(query3)
	if err != nil {
		t.Fatalf("Failed to create third plan: %v", err)
	}

	// All plans should be equivalent
	if plan1.QueryString != plan2.QueryString || plan2.QueryString != plan3.QueryString {
		t.Errorf("Normalized queries should produce same plan")
	}

	// Cache stats should show 2 hits, 1 miss
	hits, misses := planner.CacheStats()
	if hits != 2 || misses != 1 {
		t.Errorf("Expected 2 hits, 1 miss: hits=%d, misses=%d", hits, misses)
	}

	// Hit rate should be 66.67%
	hitRate := planner.CacheHitRate()
	if hitRate < 66.66 || hitRate > 66.68 {
		t.Errorf("Hit rate should be ~66.67%%, got %.2f%%", hitRate)
	}
}