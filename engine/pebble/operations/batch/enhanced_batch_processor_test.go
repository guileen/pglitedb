package batch

import (
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

func TestEnhancedBatchProcessor_Create(t *testing.T) {
	// Create a mock KV store for testing
	mockKV := &mockKV{}
	c := codec.NewMemComparableCodec()
	
	// Test default configuration
	processor := NewEnhancedBatchProcessor(mockKV, c)
	if processor == nil {
		t.Fatal("Failed to create enhanced batch processor")
	}
	
	// Test custom configuration
	config := DefaultEnhancedBatchProcessorConfig()
	config.MaxBatchSize = 100000
	config.MinBatchSize = 1000
	config.TargetBatchSize = 10000
	
	processor2 := NewEnhancedBatchProcessorWithConfig(mockKV, c, config)
	if processor2 == nil {
		t.Fatal("Failed to create enhanced batch processor with custom config")
	}
}

func TestEnhancedBatchProcessor_Configuration(t *testing.T) {
	config := DefaultEnhancedBatchProcessorConfig()
	
	// Test configuration values
	if config.MaxBatchSize != 50000 {
		t.Errorf("Expected MaxBatchSize 50000, got %d", config.MaxBatchSize)
	}
	
	if config.MinBatchSize != 500 {
		t.Errorf("Expected MinBatchSize 500, got %d", config.MinBatchSize)
	}
	
	if config.TargetBatchSize != 5000 {
		t.Errorf("Expected TargetBatchSize 5000, got %d", config.TargetBatchSize)
	}
	
	if config.AdaptiveBatchingEnabled != true {
		t.Error("Expected AdaptiveBatchingEnabled true")
	}
	
	if config.TargetLatency != 5*time.Millisecond {
		t.Errorf("Expected TargetLatency 5ms, got %v", config.TargetLatency)
	}
	
	if config.MaxLatency != 50*time.Millisecond {
		t.Errorf("Expected MaxLatency 50ms, got %v", config.MaxLatency)
	}
}

func TestEnhancedBatchProcessor_LatencyTracker(t *testing.T) {
	tracker := NewLatencyTracker(10)
	
	// Test adding latencies
	tracker.AddLatency(10 * time.Millisecond)
	tracker.AddLatency(20 * time.Millisecond)
	tracker.AddLatency(15 * time.Millisecond)
	
	// Test average latency
	avg := tracker.GetAverageLatency()
	if avg == 0 {
		t.Error("Expected non-zero average latency")
	}
	
	// Test percentiles
	percentiles := tracker.GetPercentiles([]float64{50, 95, 99})
	if len(percentiles) != 3 {
		t.Errorf("Expected 3 percentiles, got %d", len(percentiles))
	}
}

func TestEnhancedBatchProcessor_ThroughputTracker(t *testing.T) {
	tracker := NewThroughputTracker(5 * time.Second)
	
	// Test adding operations
	tracker.AddOperations(100)
	tracker.AddOperations(200)
	
	// Test throughput
	throughput := tracker.GetThroughput()
	// Throughput should be positive
	if throughput < 0 {
		t.Errorf("Expected positive throughput, got %f", throughput)
	}
}

func TestEnhancedBatchProcessor_ResourceMonitor(t *testing.T) {
	monitor := NewResourceMonitor(1024*1024*1024, 0.8) // 1GB max, 80% threshold
	
	// Test initial pressure
	pressure := monitor.GetMemoryPressure()
	if pressure != 0 {
		t.Errorf("Expected initial pressure 0, got %f", pressure)
	}
	
	// Test updating usage
	monitor.UpdateUsage(1024 * 1024 * 100) // 100MB
	pressure = monitor.GetMemoryPressure()
	if pressure <= 0 {
		t.Error("Expected positive pressure after usage update")
	}
	
	// Test reducing usage
	monitor.UpdateUsage(-(1024 * 1024 * 50)) // Reduce by 50MB
	pressure2 := monitor.GetMemoryPressure()
	if pressure2 >= pressure {
		t.Error("Expected reduced pressure after usage reduction")
	}
}

func TestEnhancedBatchProcessor_WorkloadClassifier(t *testing.T) {
	classifier := NewWorkloadClassifier("adaptive")
	
	// Test initial pattern
	pattern := classifier.GetPattern()
	if pattern == "" {
		t.Error("Expected non-empty initial pattern")
	}
	
	// Test classifying batches
	classifier.ClassifyBatch(50)   // Should classify as OLTP
	classifier.ClassifyBatch(1500) // Should classify as OLAP
	classifier.ClassifyBatch(500)  // Should classify as mixed
	
	// Test updated pattern
	pattern2 := classifier.GetPattern()
	if pattern2 == "" {
		t.Error("Expected non-empty pattern after classification")
	}
}

func TestEnhancedBatchProcessor_BufferPool(t *testing.T) {
	pool := NewBufferPool()
	
	// Test acquiring and releasing buffers
	keyBuf := pool.AcquireKeyBuffer()
	if len(keyBuf) != 0 {
		t.Errorf("Expected empty key buffer, got length %d", len(keyBuf))
	}
	
	// Add some data
	keyBuf = append(keyBuf, []byte("test_key")...)
	
	// Release buffer
	pool.ReleaseKeyBuffer(keyBuf)
	
	// Acquire again
	keyBuf2 := pool.AcquireKeyBuffer()
	if len(keyBuf2) != 0 {
		t.Errorf("Expected empty key buffer after reset, got length %d", len(keyBuf2))
	}
	
	// Release buffer
	pool.ReleaseKeyBuffer(keyBuf2)
}

func TestEnhancedBatchProcessor_MemoryManager(t *testing.T) {
	manager := NewMemoryManager(1024*1024*1024, 0.8) // 1GB max, 80% threshold
	
	// Test initial pressure
	pressure := manager.GetMemoryPressure()
	if pressure != 0 {
		t.Errorf("Expected initial pressure 0, got %f", pressure)
	}
}

func TestEnhancedBatchProcessor_QueryPatternAnalyzer(t *testing.T) {
	analyzer := NewQueryPatternAnalyzer()
	
	// Test analyzing patterns
	analyzer.AnalyzePattern("insert", 1000)
	analyzer.AnalyzePattern("select", 500)
	analyzer.AnalyzePattern("insert", 1000) // Duplicate
	
	// Analyzer should exist and accept patterns
	// Detailed testing would require accessing internal state
	// which is not exposed for simplicity
}

// Helper functions for min/max
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// mockKV is a mock implementation for testing
type mockKV struct{}

// mockBatch is a mock implementation for testing
type mockBatch struct{}