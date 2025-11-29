package sql

import (
	"testing"

	"github.com/guileen/pglitedb/benchprof"
	pg_query "github.com/pganalyze/pg_query_go/v6"
)

func BenchmarkPlanner_CreatePlan_SimpleSelect(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
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
	})
}

func BenchmarkPlanner_CreatePlan_ComplexSelect(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
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
	})
}

func BenchmarkPlanner_CreatePlan_Insert(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
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
	})
}

func BenchmarkPlanner_CreatePlan_Update(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
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
	})
}

func BenchmarkPlanner_CreatePlan_Delete(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
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
	})
}

func BenchmarkPlanner_ExtractConditionsFromExpr_Simple(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
		parser := NewPGParser()
		planner := NewPlanner(parser)
		
		// Parse a query to get the AST
		parsed, err := parser.Parse("SELECT * FROM users WHERE id = 123 AND name = 'John'")
		if err != nil {
			b.Fatal(err)
		}
		
		pgNode, ok := parsed.Statement.(*pg_query.Node)
		if !ok {
			b.Fatal("Expected pg_query.Node")
		}
		
		selectStmt := pgNode.GetSelectStmt()
		if selectStmt == nil {
			b.Fatal("Expected SelectStmt")
		}
		
		whereClause := selectStmt.GetWhereClause()
		if whereClause == nil {
			b.Fatal("Expected WHERE clause")
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conditions := planner.extractConditionsFromExpr(whereClause)
			if len(conditions) == 0 {
				b.Fatal("Expected conditions")
			}
		}
	})
}

func BenchmarkPlanner_ExtractConditionsFromExpr_Complex(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
		parser := NewPGParser()
		planner := NewPlanner(parser)
		
		// Parse a query to get the AST
		parsed, err := parser.Parse("SELECT * FROM users WHERE id = 123 AND name = 'John' AND age > 25 AND active = true")
		if err != nil {
			b.Fatal(err)
		}
		
		pgNode, ok := parsed.Statement.(*pg_query.Node)
		if !ok {
			b.Fatal("Expected pg_query.Node")
		}
		
		selectStmt := pgNode.GetSelectStmt()
		if selectStmt == nil {
			b.Fatal("Expected SelectStmt")
		}
		
		whereClause := selectStmt.GetWhereClause()
		if whereClause == nil {
			b.Fatal("Expected WHERE clause")
		}

		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			conditions := planner.extractConditionsFromExpr(whereClause)
			if len(conditions) == 0 {
				b.Fatal("Expected conditions")
			}
		}
	})
}