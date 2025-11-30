package sql

import (
	"testing"
)

func BenchmarkHybridParserCacheHitRateImprovement(b *testing.B) {
	parser := NewHybridPGParser()
	
	// Test queries with different formatting but same meaning to test cache hit rate improvement
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
		"select * from users where id = 1",  // lowercase again
		"SELECT  *  FROM  users  WHERE  id  =  1",  // more spaces
		"select * from users where id = 1;",  // lowercase with semicolon
	}
	
	// Warm up the cache with the first query
	_, err := parser.Parse(queries[0])
	if err != nil {
		b.Fatalf("Failed to parse warmup query: %v", err)
	}
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := parser.Parse(query)
		if err != nil {
			b.Fatalf("Failed to parse query: %v", err)
		}
	}
	
	// Report cache statistics
	stats := parser.GetStats()
	b.ReportMetric(float64(stats["cache_hits"]), "cache_hits")
	b.ReportMetric(float64(stats["simple_success"]), "simple_success")
	b.ReportMetric(float64(stats["fallback_count"]), "fallback_count")
}