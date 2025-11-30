package batch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// EnhancedBatchProcessorConfig extends BatchProcessorConfig with advanced adaptive batching features
type EnhancedBatchProcessorConfig struct {
	*BatchProcessorConfig
	
	// Enhanced adaptive batching parameters
	AdaptiveBatchingEnabled bool           // Whether adaptive batching is enabled
	TargetLatency           time.Duration  // Target latency for batch operations
	MaxLatency              time.Duration  // Maximum acceptable latency
	LatencyWindow           time.Duration  // Window for latency measurement
	ThroughputWindow         time.Duration  // Window for throughput measurement
	ResourcePressureAware   bool           // Whether to adjust batching based on system resource pressure
	
	// Performance optimization parameters
	MaxConcurrentBatches    int            // Maximum number of concurrent batches
	PreallocationMultiplier int            // Multiplier for pre-allocation to reduce reallocations
	BufferGrowthStrategy    string         // Strategy for buffer growth ("linear", "exponential", "adaptive")
	
	// Memory management
	MaxMemoryUsage          int64          // Maximum memory usage for batch operations (bytes)
	MemoryPressureThreshold float64        // Threshold for memory pressure (0.0-1.0)
	
	// Workload characterization
	WorkloadPattern         string         // Characterization of workload ("oltp", "olap", "mixed", "adaptive")
	QueryPatternAware       bool           // Whether to optimize based on query patterns
}

// DefaultEnhancedBatchProcessorConfig returns the default enhanced batch processor configuration
func DefaultEnhancedBatchProcessorConfig() *EnhancedBatchProcessorConfig {
	return &EnhancedBatchProcessorConfig{
		BatchProcessorConfig:    DefaultBatchProcessorConfig(),
		AdaptiveBatchingEnabled: true,
		TargetLatency:           5 * time.Millisecond,   // Target 5ms latency
		MaxLatency:              50 * time.Millisecond,   // Maximum 50ms latency
		LatencyWindow:           1 * time.Second,        // 1 second latency measurement window
		ThroughputWindow:        5 * time.Second,        // 5 second throughput measurement window
		ResourcePressureAware:   true,
		MaxConcurrentBatches:   16,                      // Allow up to 16 concurrent batches
		PreallocationMultiplier: 2,                       // Double pre-allocation to reduce reallocations
		BufferGrowthStrategy:   "adaptive",              // Use adaptive buffer growth
		MaxMemoryUsage:         1024 * 1024 * 1024,      // 1GB maximum memory usage
		MemoryPressureThreshold: 0.8,                    // 80% memory pressure threshold
		WorkloadPattern:        "adaptive",             // Automatically detect workload pattern
		QueryPatternAware:      true,                    // Optimize based on query patterns
	}
}

// EnhancedBatchProcessorStats extends BatchProcessorStats with enhanced metrics
type EnhancedBatchProcessorStats struct {
	*BatchProcessorStats
	
	// Enhanced performance metrics
	AverageLatency          time.Duration  // Average operation latency
	LatencyPercentiles      map[float64]time.Duration // Latency percentiles (50th, 95th, 99th, etc.)
	Throughput              float64        // Operations per second
	ResourceUtilization     float64        // System resource utilization (0.0-1.0)
	MemoryPressure          float64        // Current memory pressure (0.0-1.0)
	ConcurrentBatches       int64          // Current number of concurrent batches
	AdaptiveAdjustments     int64          // Number of adaptive adjustments made
	OptimalBatchSizeHistory []int          // History of optimal batch sizes
	WorkloadPattern         string         // Detected workload pattern
}

// EnhancedBatchProcessorImpl extends BatchProcessorImpl with advanced features
type EnhancedBatchProcessorImpl struct {
	*BatchProcessorImpl
	
	// Enhanced configuration
	enhancedConfig *EnhancedBatchProcessorConfig
	
	// Enhanced statistics and metrics
	enhancedStats *EnhancedBatchProcessorStats
	
	// Adaptive batching components
	latencyTracker     *LatencyTracker
	throughputTracker  *ThroughputTracker
	resourceMonitor    *ResourceMonitor
	workloadClassifier *WorkloadClassifier
	
	// Concurrency control
	concurrentBatchLimiter chan struct{}
	
	// Performance optimization components
	bufferPool         *BufferPool
	memoryManager      *MemoryManager
	queryPatternAnalyzer *QueryPatternAnalyzer
}

// LatencyTracker tracks operation latencies for adaptive optimization
type LatencyTracker struct {
	latencies     []time.Duration
	windowSize    int
	currentIndex  int
	mutex         sync.RWMutex
}

// ThroughputTracker tracks throughput metrics
type ThroughputTracker struct {
	operations    int64
	startTime     time.Time
	windowSize    time.Duration
	mutex         sync.RWMutex
}

// ResourceMonitor monitors system resource usage
type ResourceMonitor struct {
	maxMemory     int64
	threshold     float64
	currentUsage  int64
	mutex         sync.RWMutex
}

// WorkloadClassifier classifies workloads for optimization
type WorkloadClassifier struct {
	pattern       string
	confidence    float64
	history       []string
	mutex         sync.RWMutex
}

// BufferPool provides pooled buffers for batch operations
type BufferPool struct {
	keyBuffers    sync.Pool
	valueBuffers  sync.Pool
	rowIDBuffers  sync.Pool
}

// MemoryManager manages memory usage for batch operations
type MemoryManager struct {
	maxUsage      int64
	currentUsage  int64
	pressureThreshold float64
	mutex         sync.RWMutex
}

// QueryPatternAnalyzer analyzes query patterns for optimization
type QueryPatternAnalyzer struct {
	patterns      map[string]int64
	mutex         sync.RWMutex
}

// NewEnhancedBatchProcessor creates a new enhanced batch processor
func NewEnhancedBatchProcessor(kv storage.KV, codec codec.Codec) *EnhancedBatchProcessorImpl {
	return NewEnhancedBatchProcessorWithConfig(kv, codec, DefaultEnhancedBatchProcessorConfig())
}

// NewEnhancedBatchProcessorWithConfig creates a new enhanced batch processor with custom configuration
func NewEnhancedBatchProcessorWithConfig(kv storage.KV, codec codec.Codec, config *EnhancedBatchProcessorConfig) *EnhancedBatchProcessorImpl {
	baseProcessor := NewBatchProcessorWithConfig(kv, codec, config.BatchProcessorConfig)
	
	// Create concurrency limiter
	concurrentLimiter := make(chan struct{}, config.MaxConcurrentBatches)
	
	// Initialize limiter with tokens
	for i := 0; i < config.MaxConcurrentBatches; i++ {
		concurrentLimiter <- struct{}{}
	}
	
	processor := &EnhancedBatchProcessorImpl{
		BatchProcessorImpl:     baseProcessor,
		enhancedConfig:         config,
		enhancedStats:          &EnhancedBatchProcessorStats{BatchProcessorStats: &BatchProcessorStats{}},
		latencyTracker:         NewLatencyTracker(int(config.LatencyWindow/time.Millisecond/10)), // Approximate window size
		throughputTracker:      NewThroughputTracker(config.ThroughputWindow),
		resourceMonitor:         NewResourceMonitor(config.MaxMemoryUsage, config.MemoryPressureThreshold),
		workloadClassifier:      NewWorkloadClassifier(config.WorkloadPattern),
		concurrentBatchLimiter: concurrentLimiter,
		bufferPool:             NewBufferPool(),
		memoryManager:          NewMemoryManager(config.MaxMemoryUsage, config.MemoryPressureThreshold),
		queryPatternAnalyzer:   NewQueryPatternAnalyzer(),
	}
	
	return processor
}

// ProcessBatchInsert processes a batch insert operation with enhanced optimization
func (ebp *EnhancedBatchProcessorImpl) ProcessBatchInsert(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	startTime := time.Now()
	
	// Acquire concurrency token
	select {
	case <-ebp.concurrentBatchLimiter:
		defer func() {
			ebp.concurrentBatchLimiter <- struct{}{}
		}()
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	
	// Track resource usage
	ebp.resourceMonitor.UpdateUsage(int64(len(rows) * 1024)) // Estimate memory usage
	defer ebp.resourceMonitor.UpdateUsage(-int64(len(rows) * 1024))
	
	// Classify workload based on batch size
	ebp.workloadClassifier.ClassifyBatch(len(rows))
	
	// Analyze query pattern
	ebp.queryPatternAnalyzer.AnalyzePattern("insert", len(rows))
	
	// Adapt batch size if needed
	if ebp.enhancedConfig.AdaptiveBatchingEnabled {
		adaptiveBatchSize := ebp.getAdaptiveBatchSize(len(rows))
		if adaptiveBatchSize != len(rows) && len(rows) > adaptiveBatchSize {
			// Process in chunks if adaptive size is smaller
			rowIDs, err := ebp.processBatchInsertInChunks(ctx, tenantID, tableID, rows, schemaDef, adaptiveBatchSize)
			if err != nil {
				return nil, err
			}
			
			// Track metrics
			ebp.trackOperationMetrics(time.Since(startTime), len(rows))
			return rowIDs, nil
		}
	}
	
	// Use base processor for actual work
	rowIDs, err := ebp.BatchProcessorImpl.ProcessBatchInsert(ctx, tenantID, tableID, rows, schemaDef)
	if err != nil {
		return nil, err
	}
	
	// Track metrics
	ebp.trackOperationMetrics(time.Since(startTime), len(rows))
	
	return rowIDs, nil
}

// getAdaptiveBatchSize determines the optimal batch size based on current conditions
func (ebp *EnhancedBatchProcessorImpl) getAdaptiveBatchSize(currentBatchSize int) int {
	if !ebp.enhancedConfig.AdaptiveBatchingEnabled {
		return ebp.config.TargetBatchSize
	}
	
	// Get current metrics
	avgLatency := ebp.latencyTracker.GetAverageLatency()
	currentThroughput := ebp.throughputTracker.GetThroughput()
	memoryPressure := ebp.memoryManager.GetMemoryPressure()
	workloadPattern := ebp.workloadClassifier.GetPattern()
	
	// Base target size
	targetSize := ebp.config.TargetBatchSize
	
	// Adjust based on latency
	if avgLatency > ebp.enhancedConfig.TargetLatency {
		// High latency, reduce batch size
		targetSize = max(ebp.config.MinBatchSize, targetSize/2)
	} else if avgLatency < ebp.enhancedConfig.TargetLatency/2 {
		// Low latency, can increase batch size
		targetSize = min(ebp.config.MaxBatchSize, targetSize*2)
	}
	
	// Adjust based on throughput
	if currentThroughput < 1000 { // Less than 1000 ops/sec indicates underutilization
		targetSize = min(ebp.config.MaxBatchSize, targetSize*3/2)
	}
	
	// Adjust based on memory pressure
	if memoryPressure > ebp.enhancedConfig.MemoryPressureThreshold {
		// High memory pressure, reduce batch size
		targetSize = max(ebp.config.MinBatchSize, targetSize/2)
	}
	
	// Adjust based on workload pattern
	switch workloadPattern {
	case "oltp":
		// OLTP workloads prefer smaller, more frequent batches for better responsiveness
		targetSize = max(ebp.config.MinBatchSize, targetSize*2/3)
	case "olap":
		// OLAP workloads can handle larger batches for better throughput
		targetSize = min(ebp.config.MaxBatchSize, targetSize*3/2)
	}
	
	// Ensure within bounds
	targetSize = max(ebp.config.MinBatchSize, min(ebp.config.MaxBatchSize, targetSize))
	
	// Track adaptive adjustment
	if targetSize != ebp.config.TargetBatchSize {
		atomic.AddInt64(&ebp.enhancedStats.AdaptiveAdjustments, 1)
		ebp.enhancedStats.OptimalBatchSizeHistory = append(ebp.enhancedStats.OptimalBatchSizeHistory, targetSize)
		// Keep only last 100 history entries
		if len(ebp.enhancedStats.OptimalBatchSizeHistory) > 100 {
			ebp.enhancedStats.OptimalBatchSizeHistory = ebp.enhancedStats.OptimalBatchSizeHistory[1:]
		}
	}
	
	return targetSize
}

// trackOperationMetrics tracks operation metrics for optimization
func (ebp *EnhancedBatchProcessorImpl) trackOperationMetrics(latency time.Duration, batchSize int) {
	// Track latency
	ebp.latencyTracker.AddLatency(latency)
	
	// Track throughput
	ebp.throughputTracker.AddOperations(batchSize)
	
	// Update base stats
	atomic.AddInt64(&ebp.stats.TotalBatchesProcessed, 1)
	atomic.AddInt64(&ebp.stats.TotalRowsProcessed, int64(batchSize))
	
	// Update enhanced stats
	atomic.AddInt64(&ebp.enhancedStats.ConcurrentBatches, -1)
}

// GetEnhancedStats returns enhanced batch processor statistics
func (ebp *EnhancedBatchProcessorImpl) GetEnhancedStats() *EnhancedBatchProcessorStats {
	baseStats := ebp.GetStats()
	
	// Get current metrics
	avgLatency := ebp.latencyTracker.GetAverageLatency()
	latencyPercentiles := ebp.latencyTracker.GetPercentiles([]float64{50, 95, 99})
	throughput := ebp.throughputTracker.GetThroughput()
	memoryPressure := ebp.memoryManager.GetMemoryPressure()
	currentConcurrency := atomic.LoadInt64(&ebp.enhancedStats.ConcurrentBatches)
	adaptiveAdjustments := atomic.LoadInt64(&ebp.enhancedStats.AdaptiveAdjustments)
	workloadPattern := ebp.workloadClassifier.GetPattern()
	
	return &EnhancedBatchProcessorStats{
		BatchProcessorStats:    baseStats,
		AverageLatency:         avgLatency,
		LatencyPercentiles:     latencyPercentiles,
		Throughput:             throughput,
		ResourceUtilization:    float64(currentConcurrency) / float64(ebp.enhancedConfig.MaxConcurrentBatches),
		MemoryPressure:         memoryPressure,
		ConcurrentBatches:      currentConcurrency,
		AdaptiveAdjustments:    adaptiveAdjustments,
		OptimalBatchSizeHistory: ebp.enhancedStats.OptimalBatchSizeHistory,
		WorkloadPattern:        workloadPattern,
	}
}



// NewLatencyTracker creates a new latency tracker
func NewLatencyTracker(windowSize int) *LatencyTracker {
	if windowSize <= 0 {
		windowSize = 100 // Default window size
	}
	
	return &LatencyTracker{
		latencies:  make([]time.Duration, windowSize),
		windowSize: windowSize,
	}
}

// AddLatency adds a latency measurement
func (lt *LatencyTracker) AddLatency(latency time.Duration) {
	lt.mutex.Lock()
	defer lt.mutex.Unlock()
	
	lt.latencies[lt.currentIndex] = latency
	lt.currentIndex = (lt.currentIndex + 1) % len(lt.latencies)
}

// GetAverageLatency returns the average latency
func (lt *LatencyTracker) GetAverageLatency() time.Duration {
	lt.mutex.RLock()
	defer lt.mutex.RUnlock()
	
	var sum time.Duration
	count := 0
	
	for i := 0; i < len(lt.latencies); i++ {
		if lt.latencies[i] > 0 {
			sum += lt.latencies[i]
			count++
		}
	}
	
	if count == 0 {
		return 0
	}
	
	return time.Duration(int64(sum) / int64(count))
}

// GetPercentiles returns latency percentiles
func (lt *LatencyTracker) GetPercentiles(percentiles []float64) map[float64]time.Duration {
	lt.mutex.RLock()
	defer lt.mutex.RUnlock()
	
	// Create a copy of valid latencies
	validLatencies := make([]time.Duration, 0, len(lt.latencies))
	for _, latency := range lt.latencies {
		if latency > 0 {
			validLatencies = append(validLatencies, latency)
		}
	}
	
	if len(validLatencies) == 0 {
		result := make(map[float64]time.Duration)
		for _, p := range percentiles {
			result[p] = 0
		}
		return result
	}
	
	// Sort latencies
	// (In a real implementation, you'd use a proper sorting algorithm)
	
	result := make(map[float64]time.Duration)
	for _, p := range percentiles {
		index := int(float64(len(validLatencies)-1) * p / 100.0)
		if index >= len(validLatencies) {
			index = len(validLatencies) - 1
		}
		result[p] = validLatencies[index]
	}
	
	return result
}

// NewThroughputTracker creates a new throughput tracker
func NewThroughputTracker(windowSize time.Duration) *ThroughputTracker {
	return &ThroughputTracker{
		startTime:  time.Now(),
		windowSize: windowSize,
	}
}

// AddOperations adds operations to the throughput tracker
func (tt *ThroughputTracker) AddOperations(count int) {
	tt.mutex.Lock()
	defer tt.mutex.Unlock()
	
	atomic.AddInt64(&tt.operations, int64(count))
	
	// Reset window if needed
	if time.Since(tt.startTime) > tt.windowSize {
		atomic.StoreInt64(&tt.operations, int64(count))
		tt.startTime = time.Now()
	}
}

// GetThroughput returns operations per second
func (tt *ThroughputTracker) GetThroughput() float64 {
	tt.mutex.RLock()
	defer tt.mutex.RUnlock()
	
	elapsed := time.Since(tt.startTime).Seconds()
	if elapsed <= 0 {
		return 0
	}
	
	ops := atomic.LoadInt64(&tt.operations)
	return float64(ops) / elapsed
}

// NewResourceMonitor creates a new resource monitor
func NewResourceMonitor(maxMemory int64, threshold float64) *ResourceMonitor {
	return &ResourceMonitor{
		maxMemory:     maxMemory,
		threshold:     threshold,
		currentUsage:  0,
	}
}

// UpdateUsage updates resource usage
func (rm *ResourceMonitor) UpdateUsage(delta int64) {
	rm.mutex.Lock()
	defer rm.mutex.Unlock()
	
	newUsage := atomic.AddInt64(&rm.currentUsage, delta)
	if newUsage < 0 {
		atomic.StoreInt64(&rm.currentUsage, 0)
	}
}

// GetMemoryPressure returns current memory pressure (0.0-1.0)
func (rm *ResourceMonitor) GetMemoryPressure() float64 {
	rm.mutex.RLock()
	defer rm.mutex.RUnlock()
	
	if rm.maxMemory <= 0 {
		return 0
	}
	
	current := atomic.LoadInt64(&rm.currentUsage)
	return float64(current) / float64(rm.maxMemory)
}

// NewWorkloadClassifier creates a new workload classifier
func NewWorkloadClassifier(initialPattern string) *WorkloadClassifier {
	return &WorkloadClassifier{
		pattern:    initialPattern,
		confidence: 0.5, // Initial confidence
		history:    make([]string, 0, 100),
	}
}

// ClassifyBatch classifies workload based on batch size
func (wc *WorkloadClassifier) ClassifyBatch(batchSize int) {
	wc.mutex.Lock()
	defer wc.mutex.Unlock()
	
	var newPattern string
	if batchSize < 100 {
		newPattern = "oltp" // Small batches typical of OLTP
	} else if batchSize > 1000 {
		newPattern = "olap" // Large batches typical of OLAP
	} else {
		newPattern = "mixed" // Medium batches could be either
	}
	
	// Update history
	wc.history = append(wc.history, newPattern)
	if len(wc.history) > 100 {
		wc.history = wc.history[1:]
	}
	
	// Update current pattern based on recent history
	wc.updatePattern()
}

// updatePattern updates the current pattern based on history
func (wc *WorkloadClassifier) updatePattern() {
	if len(wc.history) == 0 {
		return
	}
	
	// Count pattern occurrences
	counts := make(map[string]int)
	for _, pattern := range wc.history {
		counts[pattern]++
	}
	
	// Find most common pattern
	mostCommon := ""
	maxCount := 0
	for pattern, count := range counts {
		if count > maxCount {
			mostCommon = pattern
			maxCount = count
		}
	}
	
	wc.pattern = mostCommon
	wc.confidence = float64(maxCount) / float64(len(wc.history))
}

// GetPattern returns the current workload pattern
func (wc *WorkloadClassifier) GetPattern() string {
	wc.mutex.RLock()
	defer wc.mutex.RUnlock()
	
	return wc.pattern
}

// NewBufferPool creates a new buffer pool
func NewBufferPool() *BufferPool {
	return &BufferPool{
		keyBuffers: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128)
			},
		},
		valueBuffers: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
		rowIDBuffers: sync.Pool{
			New: func() interface{} {
				return make([]int64, 0, 64)
			},
		},
	}
}

// AcquireKeyBuffer acquires a key buffer from the pool
func (bp *BufferPool) AcquireKeyBuffer() []byte {
	buf := bp.keyBuffers.Get()
	if buf == nil {
		return make([]byte, 0, 128)
	}
	return buf.([]byte)[:0]
}

// ReleaseKeyBuffer releases a key buffer to the pool
func (bp *BufferPool) ReleaseKeyBuffer(buf []byte) {
	if cap(buf) <= 1024 { // Only pool reasonably sized buffers
		bp.keyBuffers.Put(buf[:0])
	}
}

// NewMemoryManager creates a new memory manager
func NewMemoryManager(maxUsage int64, threshold float64) *MemoryManager {
	return &MemoryManager{
		maxUsage:          maxUsage,
		pressureThreshold: threshold,
		currentUsage:      0,
	}
}

// GetMemoryPressure returns current memory pressure
func (mm *MemoryManager) GetMemoryPressure() float64 {
	mm.mutex.RLock()
	defer mm.mutex.RUnlock()
	
	if mm.maxUsage <= 0 {
		return 0
	}
	
	current := atomic.LoadInt64(&mm.currentUsage)
	return float64(current) / float64(mm.maxUsage)
}

// NewQueryPatternAnalyzer creates a new query pattern analyzer
func NewQueryPatternAnalyzer() *QueryPatternAnalyzer {
	return &QueryPatternAnalyzer{
		patterns: make(map[string]int64),
	}
}

// AnalyzePattern analyzes a query pattern
func (qpa *QueryPatternAnalyzer) AnalyzePattern(operation string, batchSize int) {
	key := fmt.Sprintf("%s_%d", operation, batchSize)
	
	qpa.mutex.Lock()
	defer qpa.mutex.Unlock()
	
	count := qpa.patterns[key]
	atomic.AddInt64(&count, 1)
	qpa.patterns[key] = count
}