package sql

import (
	"testing"
)

func BenchmarkEnhancedPlanner_CreatePlan(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	// Test queries
	queries := []string{
		"SELECT * FROM users",
		"SELECT name FROM users WHERE id = 1",
		"UPDATE users SET name = 'test' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
		"INSERT INTO users (name, email) VALUES ('test', 'test@example.com')",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
}

func BenchmarkEnhancedPlanner_CacheHitRate(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	// Warm up cache with repeated queries
	query := "SELECT * FROM users WHERE id = 1"
	for i := 0; i < 100; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
	
	b.ResetTimer()
	
	// Measure cache hit rate
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
}

func BenchmarkEnhancedPlanner_DependencyTracking(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewEnhancedPlanner(parser)
	
	queries := []string{
		"SELECT * FROM users",
		"SELECT name FROM users WHERE id = 1",
		"UPDATE users SET name = 'test' WHERE id = 1",
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
}