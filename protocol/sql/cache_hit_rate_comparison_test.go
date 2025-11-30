package sql

import (
	"testing"
)

func BenchmarkPlannerCacheHitRate_BeforeNormalization(b *testing.B) {
	// Simulate the old behavior where cache keys were not normalized
	// This would be similar to having queries with different formatting
	
	// Create a planner with simple parser
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	
	// Test queries with identical meaning but different formatting
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
	}
	
	// Warm up - parse each unique query once
	for _, query := range queries {
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create warmup plan: %v", err)
		}
	}
	
	// Reset stats to measure cache hits
	planner.ResetCacheStats()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
	
	// In the old system, these would mostly be cache misses
	hits, misses := planner.CacheStats()
	b.ReportMetric(float64(hits), "cache_hits")
	b.ReportMetric(float64(misses), "cache_misses")
	hitRate := planner.CacheHitRate()
	b.ReportMetric(hitRate, "hit_rate_percentage")
}

func BenchmarkPlannerCacheHitRate_AfterNormalization(b *testing.B) {
	// This represents the new behavior with enhanced normalization
	
	// Create a planner with hybrid parser (which now uses normalization)
	parser := NewHybridPGParser()
	planner := NewPlanner(parser)
	
	// Test queries with identical meaning but different formatting
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
	}
	
	// Warm up - parse the first query to populate cache
	_, err := planner.CreatePlan(queries[0])
	if err != nil {
		b.Fatalf("Failed to create warmup plan: %v", err)
	}
	
	// Reset stats to measure cache hits
	planner.ResetCacheStats()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		_, err := planner.CreatePlan(query)
		if err != nil {
			b.Fatalf("Failed to create plan: %v", err)
		}
	}
	
	// With normalization, these should mostly be cache hits
	hits, misses := planner.CacheStats()
	b.ReportMetric(float64(hits), "cache_hits")
	b.ReportMetric(float64(misses), "cache_misses")
	hitRate := planner.CacheHitRate()
	b.ReportMetric(hitRate, "hit_rate_percentage")
}