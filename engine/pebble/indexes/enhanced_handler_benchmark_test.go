package indexes

import (
	"testing"
)

func BenchmarkEnhancedHandler_BufferPool(b *testing.B) {
	pool := NewIndexBufferPool()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		buf := pool.AcquireIndexKeyBuffer()
		buf = append(buf, []byte("test_data")...)
		pool.ReleaseIndexKeyBuffer(buf)
	}
}

func BenchmarkEnhancedHandler_IndexStats(b *testing.B) {
	tracker := NewIndexStatsTracker()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		tracker.UpdateLookupStats(1, 1, 1, int64(i%1000000))
		_ = tracker.GetIndexStats(1, 1, 1)
	}
}

func BenchmarkEnhancedHandler_AdaptiveBuilder(b *testing.B) {
	tracker := NewIndexStatsTracker()
	builder := NewAdaptiveIndexBuilder(tracker)
	
	// Warm up with some stats
	for i := 0; i < 1000; i++ {
		tracker.UpdateLookupStats(1, 1, 1, int64(i%1000000))
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = builder.ShouldBuildIndex(1, 1, 1)
	}
}