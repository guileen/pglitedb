package sql

import (
	"testing"
)

func TestEnhancedPlanner_Create(t *testing.T) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	if planner == nil {
		t.Fatal("Failed to create enhanced planner")
	}
	
	// Test with nil catalog (should work with default planner)
	plannerWithCatalog := NewEnhancedPlannerWithCatalog(parser, nil)
	
	if plannerWithCatalog == nil {
		t.Fatal("Failed to create enhanced planner with nil catalog")
	}
}

func TestEnhancedPlanner_CreatePlan(t *testing.T) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	// Test simple SELECT query
	query := "SELECT * FROM users"
	plan, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}
	
	if plan == nil {
		t.Fatal("Got nil plan")
	}
	
	if plan.Operation != "select" {
		t.Errorf("Expected operation 'select', got '%s'", plan.Operation)
	}
	
	// Test that plan is cached
	plan2, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create second plan: %v", err)
	}
	
	// Plans should be equivalent but not the same object (due to copying)
	if plan2 == plan {
		t.Error("Expected different plan objects due to copying")
	}
	
	// Check metrics
	metrics := planner.GetEnhancedMetrics()
	if metrics.AdaptiveCacheMisses != 1 {
		t.Errorf("Expected 1 cache miss, got %d", metrics.AdaptiveCacheMisses)
	}
	if metrics.AdaptiveCacheHits != 1 {
		t.Errorf("Expected 1 cache hit, got %d", metrics.AdaptiveCacheHits)
	}
}

func TestEnhancedPlanner_DependencyTracking(t *testing.T) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	// Create a plan to trigger dependency tracking
	query := "SELECT id, name FROM users WHERE age > 18"
	_, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}
	
	// Test dependency tracking
	// This is a simplified test - in practice, you'd want to verify
	// that dependencies are properly tracked
	tracker := planner.planDependencyTracker
	if tracker == nil {
		t.Error("Dependency tracker is nil")
	}
}

func TestEnhancedPlanner_CacheInvalidation(t *testing.T) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	// Create some plans
	queries := []string{
		"SELECT * FROM users",
		"SELECT name FROM users WHERE id = 1",
		"UPDATE users SET name = 'test' WHERE id = 1",
	}
	
	for _, query := range queries {
		_, err := planner.CreatePlan(query)
		if err != nil {
			t.Fatalf("Failed to create plan for query '%s': %v", query, err)
		}
	}
	
	// Test cache invalidation
	planner.InvalidateCache("table:users")
	
	// Check that invalidation was recorded
	metrics := planner.GetEnhancedMetrics()
	if metrics.CacheInvalidations != 1 {
		t.Errorf("Expected 1 cache invalidation, got %d", metrics.CacheInvalidations)
	}
}