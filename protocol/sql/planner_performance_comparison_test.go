package sql

import (
	"testing"
)

// Benchmark to compare planner performance with and without effective caching
func BenchmarkPlannerPerformanceComparison(b *testing.B) {
	// Test with a variety of queries that should benefit from normalization
	queries := []string{
		"SELECT id, name FROM users WHERE age > 25",
		"select id, name from users where age > 25",           // Different case
		"  SELECT   id,   name   FROM   users   WHERE   age   >   25  ", // Extra spaces
		"SELECT id,name FROM users WHERE age>25",              // No spaces
		"SELECT id, name FROM users WHERE age > 25;",          // With semicolon
		"select ID, NAME from USERS where AGE > 25",           // Mixed case variations
		"SELECT  id,  name  FROM  users  WHERE  age  >  25",   // More spaces
	}

	b.Run("WithEnhancedNormalization", func(b *testing.B) {
		parser := NewHybridPGParser()
		planner := NewPlanner(parser)
		
		// Warm up with first query
		_, err := planner.CreatePlan(queries[0])
		if err != nil {
			b.Fatalf("Failed warmup: %v", err)
		}
		
		planner.ResetCacheStats()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			_, err := planner.CreatePlan(query)
			if err != nil {
				b.Fatalf("Failed to create plan: %v", err)
			}
		}
		
		hits, misses := planner.CacheStats()
		b.ReportMetric(float64(hits), "cache_hits")
		b.ReportMetric(float64(misses), "cache_misses")
		b.ReportMetric(planner.CacheHitRate(), "hit_rate_percentage")
	})

	b.Run("WithoutParserLevelNormalization", func(b *testing.B) {
		// Simulate the old behavior by using a simple parser without hybrid benefits
		parser := NewSimplePGParser()
		planner := NewPlanner(parser)
		
		// Warm up with first query
		_, err := planner.CreatePlan(queries[0])
		if err != nil {
			b.Fatalf("Failed warmup: %v", err)
		}
		
		planner.ResetCacheStats()
		
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			query := queries[i%len(queries)]
			_, err := planner.CreatePlan(query)
			if err != nil {
				b.Fatalf("Failed to create plan: %v", err)
			}
		}
		
		hits, misses := planner.CacheStats()
		b.ReportMetric(float64(hits), "cache_hits")
		b.ReportMetric(float64(misses), "cache_misses")
		b.ReportMetric(planner.CacheHitRate(), "hit_rate_percentage")
	})
}