package sql

import (
	"testing"
)

func BenchmarkPlannerCachePerformance_BeforeImprovements(b *testing.B) {
	// Simulate the performance with the original implementation
	parser := NewSimplePGParser()
	planner := NewPlanner(parser)
	
	// Test queries with various formatting to test cache effectiveness
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
		"SELECT u.id, u.name FROM users u WHERE u.age > 25 AND u.status = 'active'",
		"select u.id, u.name from users u where u.age > 25 and u.status = 'active'", // lowercase
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
	
	// Report cache statistics
	hits, misses := planner.CacheStats()
	b.ReportMetric(float64(hits), "cache_hits")
	b.ReportMetric(float64(misses), "cache_misses")
	hitRate := planner.CacheHitRate()
	b.ReportMetric(hitRate, "hit_rate_percentage")
}

func BenchmarkPlannerCachePerformance_AfterImprovements(b *testing.B) {
	// Test with the improved implementation
	parser := NewHybridPGParser()
	planner := NewPlanner(parser)
	
	// Test queries with various formatting to test cache effectiveness
	queries := []string{
		"SELECT * FROM users WHERE id = 1",
		"select * from users where id = 1",  // lowercase
		"  SELECT   *   FROM   users   WHERE   id   =   1  ",  // extra spaces
		"SELECT * FROM users WHERE id=1",  // no spaces around =
		"SELECT * FROM users WHERE id = 1;",  // with semicolon
		"SELECT u.id, u.name FROM users u WHERE u.age > 25 AND u.status = 'active'",
		"select u.id, u.name from users u where u.age > 25 and u.status = 'active'", // lowercase
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
	
	// Report cache statistics
	hits, misses := planner.CacheStats()
	b.ReportMetric(float64(hits), "cache_hits")
	b.ReportMetric(float64(misses), "cache_misses")
	hitRate := planner.CacheHitRate()
	b.ReportMetric(hitRate, "hit_rate_percentage")
}

func BenchmarkLRUCachePerformance_Improved(b *testing.B) {
	cache := NewLRUCache(1000)
	
	// Pre-populate cache
	for i := 0; i < 100; i++ {
		cache.Put("key"+string(rune(i)), "value"+string(rune(i)))
	}
	
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := "key" + string(rune(i%100))
			cache.Get(key)
			i++
		}
	})
}