package sql

import (
	"testing"
)

func BenchmarkHybridParserSmartDecisions(b *testing.B) {
	parser := NewHybridPGParser()

	// Test queries that should be handled by the simple parser
	simpleQueries := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')",
		"UPDATE users SET name = 'Bob' WHERE id = 1",
		"DELETE FROM users WHERE id = 1",
	}

	// Test queries that should fall back to the full parser
	complexQueries := []string{
		"SELECT u.name, p.title FROM users u JOIN posts p ON u.id = p.user_id",
		"SELECT department, COUNT(*) FROM employees GROUP BY department",
		"SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)",
	}

	b.Run("SimpleQueries", func(b *testing.B) {
		statsBefore := parser.GetStats()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := simpleQueries[i%len(simpleQueries)]
			_, err := parser.Parse(query)
			if err != nil {
				b.Fatalf("Failed to parse query: %v", err)
			}
		}
		
		statsAfter := parser.GetStats()
		b.ReportMetric(float64(statsAfter["simple_success"]-statsBefore["simple_success"]), "simple_parser_success")
		b.ReportMetric(float64(statsAfter["fallback_count"]-statsBefore["fallback_count"]), "full_parser_fallbacks")
	})

	b.Run("ComplexQueries", func(b *testing.B) {
		statsBefore := parser.GetStats()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := complexQueries[i%len(complexQueries)]
			_, err := parser.Parse(query)
			if err != nil {
				b.Fatalf("Failed to parse query: %v", err)
			}
		}
		
		statsAfter := parser.GetStats()
		b.ReportMetric(float64(statsAfter["simple_success"]-statsBefore["simple_success"]), "simple_parser_success")
		b.ReportMetric(float64(statsAfter["fallback_count"]-statsBefore["fallback_count"]), "full_parser_fallbacks")
	})
}