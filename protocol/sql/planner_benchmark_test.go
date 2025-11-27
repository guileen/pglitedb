package sql

import (
	"testing"
)

func BenchmarkPlanner_CreatePlan_SimpleSelect(b *testing.B) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	query := "SELECT id, name, email FROM users WHERE id = 123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanner_CreatePlan_ComplexSelect(b *testing.B) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	query := "SELECT u.name, u.email, p.title FROM users u JOIN posts p ON u.id = p.user_id WHERE u.age > 18 AND u.city IN ('Beijing', 'Shanghai', 'Guangzhou') AND p.created_at BETWEEN '2023-01-01' AND '2023-12-31' AND u.active = true ORDER BY p.created_at DESC LIMIT 10"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanner_CreatePlan_Insert(b *testing.B) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	query := "INSERT INTO users (name, email, age, active) VALUES ('Alice', 'alice@example.com', 30, true)"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanner_CreatePlan_Update(b *testing.B) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	query := "UPDATE users SET age = 31, active = false WHERE id = 123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPlanner_CreatePlan_Delete(b *testing.B) {
	parser := NewPGParser()
	planner := NewPlanner(parser)
	query := "DELETE FROM users WHERE id = 123"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatal(err)
		}
	}
}