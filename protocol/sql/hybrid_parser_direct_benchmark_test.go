package sql

import (
	"testing"
)

// Benchmark to directly compare hybrid parser performance with and without normalization
func BenchmarkHybridParserDirect(b *testing.B) {
	// Test queries that should benefit from normalization
	queries := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"select id, name from users where age > 25",           // Different case
		"  SELECT   id,   name   FROM   users   WHERE   age   >   25  ", // Extra spaces
		"SELECT id,name FROM users WHERE age>25",              // No spaces
		"SELECT id, name FROM users WHERE age > 25;",          // With semicolon
		"select ID, NAME from USERS where AGE > 25",           // Mixed case variations
		"SELECT  id,  name  FROM  users  WHERE  age  >  25",   // More spaces
	}

	b.Run("ParserLevelCachingWithNormalization", func(b *testing.B) {
		parser := NewHybridPGParser()
		
		// Warm up with first query
		_, err := parser.Parse(queries[0])
		if err != nil {
			b.Fatalf("Failed warmup: %v", err)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			_, err := parser.Parse(query)
			if err != nil {
				b.Fatalf("Failed to parse: %v", err)
			}
		}
		
		stats := parser.GetStats()
		b.ReportMetric(float64(stats["cache_hits"]), "parser_cache_hits")
		b.ReportMetric(float64(stats["simple_success"]), "simple_parser_success")
		b.ReportMetric(float64(stats["fallback_count"]), "full_parser_fallbacks")
	})

	b.Run("ParserLevelWithoutCachingBenefits", func(b *testing.B) {
		// This simulates what would happen without normalization by using queries that
		// are truly different and can't benefit from caching
		queries := []string{
			"SELECT id, name FROM users WHERE age > 25",
			"SELECT id, email FROM users WHERE age > 30",
			"SELECT name, age FROM users WHERE active = true",
			"SELECT * FROM products WHERE price < 100",
			"SELECT id, title FROM posts WHERE published = true",
			"SELECT user_id, role FROM user_roles WHERE active = true",
			"SELECT category, count FROM categories WHERE visible = true",
		}
		
		parser := NewHybridPGParser()
		
		// Warm up with first query
		_, err := parser.Parse(queries[0])
		if err != nil {
			b.Fatalf("Failed warmup: %v", err)
		}
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			_, err := parser.Parse(query)
			if err != nil {
				b.Fatalf("Failed to parse: %v", err)
			}
		}
		
		stats := parser.GetStats()
		b.ReportMetric(float64(stats["cache_hits"]), "parser_cache_hits")
		b.ReportMetric(float64(stats["simple_success"]), "simple_parser_success")
		b.ReportMetric(float64(stats["fallback_count"]), "full_parser_fallbacks")
	})
}