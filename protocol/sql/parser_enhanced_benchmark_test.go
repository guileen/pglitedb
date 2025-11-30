package sql

import (
	"testing"
)

func BenchmarkSimpleParserEnhanced(b *testing.B) {
	parser := NewSimplePGParser()

	queries := []string{
		"SELECT id, name, email FROM users WHERE age > 25 AND active = true ORDER BY created_at DESC LIMIT 100",
		"INSERT INTO users (name, email, age) VALUES ('Alice', 'alice@example.com', 30)",
		"UPDATE users SET name = 'Bob', email = 'bob@example.com' WHERE id = 1",
		"DELETE FROM users WHERE id = 1 AND active = false",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := parser.Parse(query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}
	}
}

func BenchmarkSimpleParserBasic(b *testing.B) {
	parser := NewSimplePGParser()

	// Simple queries that the original parser could handle
	queries := []string{
		"SELECT * FROM users",
		"INSERT INTO users (name) VALUES ('Alice')",
		"UPDATE users SET name = 'Bob'",
		"DELETE FROM users",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := parser.Parse(query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}
	}
}