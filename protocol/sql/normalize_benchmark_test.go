package sql

import (
	"testing"
)

func BenchmarkNormalizeQuery(b *testing.B) {
	query := "SELECT id, name, email FROM users WHERE age > 25 AND status = 'active' ORDER BY created_at DESC LIMIT 100"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = NormalizeQuery(query)
	}
}

func BenchmarkHybridParserWithNormalization(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test queries with different formatting but same meaning
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
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

func BenchmarkHybridParserWithoutNormalization(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test with exactly the same query each time (best case for old cache)
	query := "SELECT * FROM users WHERE id = 1"
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := parser.Parse(query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}
	}
}