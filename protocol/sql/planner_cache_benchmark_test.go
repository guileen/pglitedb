package sql

import (
	"testing"
)

func BenchmarkPlannerWithoutCaching(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	planner.EnablePlanCaching(false) // Disable caching
	
	query := "SELECT id, name, email FROM users WHERE age > 18 AND status = 'active' ORDER BY created_at LIMIT 100"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
}

func BenchmarkPlannerWithCaching(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	planner.EnablePlanCaching(true) // Enable caching
	
	query := "SELECT id, name, email FROM users WHERE age > 18 AND status = 'active' ORDER BY created_at LIMIT 100"
	
	// Pre-populate cache
	_, err := planner.CreatePlan(query)
	if err != nil {
		b.Fatalf("Failed to create initial plan: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
}

func BenchmarkPlannerMixedWorkload(b *testing.B) {
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	planner.EnablePlanCaching(true) // Enable caching
	
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"SELECT * FROM users WHERE id = 2",
		"SELECT * FROM users WHERE id = 3",
		"SELECT * FROM products WHERE category = 'electronics'",
		"SELECT * FROM products WHERE category = 'books'",
		"INSERT INTO users (name, email) VALUES ('John Doe', 'john@example.com')",
		"UPDATE users SET status = 'active' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	}
	
	// Pre-populate cache with all queries
	for _, query := range queries {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create initial plan: %v", err)
		}
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