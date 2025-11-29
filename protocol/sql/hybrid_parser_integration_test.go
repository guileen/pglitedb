package sql

import (
	"testing"
)

func TestHybridParserIntegration(t *testing.T) {
	// Test that the hybrid parser works with the planner
	parser := NewHybridPGParser()
	planner := NewPlanner(parser)
	
	queries := []string{
		"SELECT id, name FROM users WHERE id = 1",
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	}
	
	for _, query := range queries {
		t.Run(query, func(t *testing.T) {
			plan, err := planner.CreatePlan(query)
			if err != nil {
				t.Fatalf("Failed to create plan for query '%s': %v", query, err)
			}
			
			if plan == nil {
				t.Fatalf("Plan is nil for query '%s'", query)
			}
			
			if plan.QueryString != query {
				t.Errorf("Expected query string '%s', got '%s'", query, plan.QueryString)
			}
		})
	}
}

func TestPlannerWithNilParser(t *testing.T) {
	// Test that the planner uses hybrid parser by default
	planner := NewPlanner(nil)
	
	query := "SELECT id, name FROM users"
	plan, err := planner.CreatePlan(query)
	if err != nil {
		t.Fatalf("Failed to create plan: %v", err)
	}
	
	if plan == nil {
		t.Fatal("Plan is nil")
	}
	
	if plan.QueryString != query {
		t.Errorf("Expected query string '%s', got '%s'", query, plan.QueryString)
	}
}