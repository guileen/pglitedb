package batch

import (
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
)

func BenchmarkEnhancedBatchProcessor_LatencyTracker(b *testing.B) {
	tracker := NewLatencyTracker(1000)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		latency := time.Duration(i%100) * time.Millisecond
		tracker.AddLatency(latency)
		_ = tracker.GetAverageLatency()
	}
}

func BenchmarkEnhancedBatchProcessor_ThroughputTracker(b *testing.B) {
	tracker := NewThroughputTracker(5 * time.Second)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		tracker.AddOperations(100)
		_ = tracker.GetThroughput()
	}
}

func BenchmarkEnhancedBatchProcessor_ResourceMonitor(b *testing.B) {
	monitor := NewResourceMonitor(1024*1024*1024, 0.8)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		monitor.UpdateUsage(int64(i % 1000000))
		_ = monitor.GetMemoryPressure()
	}
}

func BenchmarkEnhancedBatchProcessor_WorkloadClassifier(b *testing.B) {
	classifier := NewWorkloadClassifier("adaptive")
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		batchSize := i % 2000
		classifier.ClassifyBatch(batchSize)
		_ = classifier.GetPattern()
	}
}

func BenchmarkEnhancedBatchProcessor_BufferPool(b *testing.B) {
	pool := NewBufferPool()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		buf := pool.AcquireKeyBuffer()
		buf = append(buf, []byte("test_key_data")...)
		pool.ReleaseKeyBuffer(buf)
	}
}

func BenchmarkEnhancedBatchProcessor_MemoryManager(b *testing.B) {
	manager := NewMemoryManager(1024*1024*1024, 0.8)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = manager.GetMemoryPressure()
	}
}

func BenchmarkEnhancedBatchProcessor_QueryPatternAnalyzer(b *testing.B) {
	analyzer := NewQueryPatternAnalyzer()
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		batchSize := i % 10000
		analyzer.AnalyzePattern("insert", batchSize)
	}
}

func BenchmarkEnhancedBatchProcessor_AdaptiveBatchSize(b *testing.B) {
	// Create a mock setup for testing
	mockKV := &mockKV{}
	c := codec.NewMemComparableCodec()
	config := DefaultEnhancedBatchProcessorConfig()
	
	processor := NewEnhancedBatchProcessorWithConfig(mockKV, c, config)
	
	// Warm up with some data
	for i := 0; i < 1000; i++ {
		latency := time.Duration(i%50) * time.Millisecond
		processor.latencyTracker.AddLatency(latency)
		processor.throughputTracker.AddOperations(1000)
		processor.memoryManager.UpdateUsage(int64((i % 100) * 1024 * 1024))
	}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		batchSize := i % 10000
		_ = processor.getAdaptiveBatchSize(batchSize)
	}
}