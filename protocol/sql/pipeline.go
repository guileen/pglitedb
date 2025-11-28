// Advanced query execution pipeline with optimized batch processing and resource management
package sql

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/types"
)

// QueryPipeline manages batched query execution with advanced optimization
type QueryPipeline struct {
	executor     *Executor
	batchSize    int
	maxQueueSize int
	buffer       []*PipelineQuery
	mu           sync.Mutex
	flushTimer   *time.Timer
	stats        PipelineStats
	workerPool   *WorkerPool
	
	// Dynamic batching configuration
	config       PipelineConfig
	lastAdjustment time.Time
}

// PipelineStats contains statistics about the pipeline performance
type PipelineStats struct {
	QueriesProcessed uint64
	BatchesProcessed uint64
	AverageBatchSize uint64 // Store as uint64 for atomic operations
	QueueLength      int64
	WorkersActive    int64
	CurrentBatchSize int64 // Current dynamic batch size
}

// PipelineConfig holds configuration for the query pipeline's dynamic batching
//
// The configuration enables fine-tuning of the adaptive batching behavior:
// - BaseBatchSize: Starting batch size for query processing
// - EnableDynamicBatching: Toggle for enabling/disabling adaptive behavior
// - Min/MaxBatchSize: Bounds for dynamic batch size adjustment
// - AdjustmentInterval: Rate limiting for batch size changes
// - Queue thresholds: Control when to increase/decrease batch size
// - Scaling factors: Determine magnitude of batch size adjustments
type PipelineConfig struct {
	// Base batch size for query processing
	BaseBatchSize int
	
	// Dynamic batching parameters
	EnableDynamicBatching bool          // Whether to enable dynamic batch sizing
	MinBatchSize          int          // Minimum batch size
	MaxBatchSize          int          // Maximum batch size
	AdjustmentInterval    time.Duration // How often to adjust batch size
	QueueGrowthThreshold  float64      // Queue length growth threshold to increase batch size
	QueueShrinkThreshold  float64      // Queue length shrink threshold to decrease batch size
	BatchSizeIncreaseFactor float64    // Factor by which to increase batch size
	BatchSizeDecreaseFactor float64    // Factor by which to decrease batch size
}

// PipelineQuery represents a query in the pipeline
type PipelineQuery struct {
	Context   context.Context
	Query     string
	Result    chan *PipelineResult
	SubmitTime time.Time
}

// PipelineResult represents the result of a pipelined query
type PipelineResult struct {
	ResultSet *types.ResultSet
	Error     error
	Duration  time.Duration
}

// DefaultPipelineConfig returns the default configuration for the query pipeline
func DefaultPipelineConfig() PipelineConfig {
	return PipelineConfig{
		BaseBatchSize:           10,
		EnableDynamicBatching:   true,
		MinBatchSize:            5,
		MaxBatchSize:            100,
		AdjustmentInterval:      100 * time.Millisecond,
		QueueGrowthThreshold:    0.8,  // Increase batch size when queue is 80% full
		QueueShrinkThreshold:    0.2,  // Decrease batch size when queue is 20% full
		BatchSizeIncreaseFactor: 1.5,  // Increase batch size by 50%
		BatchSizeDecreaseFactor: 0.8,  // Decrease batch size by 20%
	}
}

// WorkerPool manages a pool of workers for executing query batches
type WorkerPool struct {
	workers   int
	jobQueue  chan *BatchJob
	wg        sync.WaitGroup
	ctx       context.Context
	cancel    context.CancelFunc
}

// BatchJob represents a batch of queries to be executed
type BatchJob struct {
	Queries []*PipelineQuery
	Pipeline *QueryPipeline
}

// NewQueryPipeline creates a new query execution pipeline with advanced optimization
func NewQueryPipeline(executor *Executor, batchSize int) *QueryPipeline {
	return NewQueryPipelineWithConfig(executor, PipelineConfig{BaseBatchSize: batchSize})
}

// NewQueryPipelineWithConfig creates a new query execution pipeline with custom configuration
//
// This function initializes a query pipeline with dynamic batching capabilities:
// - Worker pool for parallel batch processing
// - Configurable batch sizing parameters
// - Adaptive batch sizing based on workload patterns
// - Statistics collection for performance monitoring
func NewQueryPipelineWithConfig(executor *Executor, config PipelineConfig) *QueryPipeline {
	if config.BaseBatchSize <= 0 {
		config.BaseBatchSize = 10
	}
	
	// Apply defaults for missing configuration values
	defaultConfig := DefaultPipelineConfig()
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = defaultConfig.MinBatchSize
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = defaultConfig.MaxBatchSize
	}
	if config.AdjustmentInterval <= 0 {
		config.AdjustmentInterval = defaultConfig.AdjustmentInterval
	}
	if config.QueueGrowthThreshold <= 0 {
		config.QueueGrowthThreshold = defaultConfig.QueueGrowthThreshold
	}
	if config.QueueShrinkThreshold <= 0 {
		config.QueueShrinkThreshold = defaultConfig.QueueShrinkThreshold
	}
	if config.BatchSizeIncreaseFactor <= 1.0 {
		config.BatchSizeIncreaseFactor = defaultConfig.BatchSizeIncreaseFactor
	}
	if config.BatchSizeDecreaseFactor >= 1.0 || config.BatchSizeDecreaseFactor <= 0 {
		config.BatchSizeDecreaseFactor = defaultConfig.BatchSizeDecreaseFactor
	}
	
	// Determine optimal worker count based on CPU cores
	workerCount := runtime.NumCPU()
	if workerCount > 8 {
		workerCount = 8 // Cap at 8 workers to avoid excessive context switching
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	wp := &WorkerPool{
		workers:  workerCount,
		jobQueue: make(chan *BatchJob, workerCount*2), // Buffer for 2 jobs per worker
		ctx:      ctx,
		cancel:   cancel,
	}
	
	// Start workers
	for i := 0; i < workerCount; i++ {
		wp.wg.Add(1)
		go wp.worker()
	}
	
	initialBatchSize := config.BaseBatchSize
	if !config.EnableDynamicBatching {
		// If dynamic batching is disabled, use fixed batch size
		config.MinBatchSize = initialBatchSize
		config.MaxBatchSize = initialBatchSize
	}
	
	return &QueryPipeline{
		executor:     executor,
		batchSize:    initialBatchSize,
		maxQueueSize: initialBatchSize * 4, // Allow up to 4 batches in queue
		buffer:       make([]*PipelineQuery, 0, initialBatchSize*2),
		workerPool:   wp,
		config:       config,
		lastAdjustment: time.Now(),
	}
}

// Execute adds a query to the pipeline and returns the result
//
// This method implements adaptive batching to optimize performance:
// - Queries are buffered and processed in batches for efficiency
// - Batch size dynamically adjusts based on queue utilization
// - Under high load, batch sizes increase to maximize throughput
// - Under low load, batch sizes decrease to minimize latency
// - Queue overflow is handled by executing queries immediately
func (p *QueryPipeline) Execute(ctx context.Context, query string) (*types.ResultSet, error) {
	atomic.AddInt64(&p.stats.QueueLength, 1)
	defer atomic.AddInt64(&p.stats.QueueLength, -1)
	
	resultChan := make(chan *PipelineResult, 1)
	pq := &PipelineQuery{
		Context:   ctx,
		Query:     query,
		Result:    resultChan,
		SubmitTime: time.Now(),
	}

	p.mu.Lock()
	
	// Check if we need to reject due to queue overflow
	if len(p.buffer) >= p.maxQueueSize {
		p.mu.Unlock()
		// Queue is full, execute immediately without batching
		result, err := p.executor.Execute(ctx, query)
		return result, err
	}
	
	p.buffer = append(p.buffer, pq)
	bufferLen := len(p.buffer)
	
	// Adjust batch size based on current queue length if dynamic batching is enabled
	if p.config.EnableDynamicBatching {
		p.adjustBatchSizeLocked()
	}
	
	// Check if we need to flush
	if bufferLen >= p.batchSize {
		p.flushLocked()
	} else if p.flushTimer == nil {
		// Schedule a flush for later to prevent indefinite queuing
		p.flushTimer = time.AfterFunc(5*time.Millisecond, p.flush)
	}
	p.mu.Unlock()

	// Wait for result with context awareness
	select {
	case result := <-resultChan:
		atomic.AddUint64(&p.stats.QueriesProcessed, 1)
		return result.ResultSet, result.Error
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// flush processes all buffered queries
func (p *QueryPipeline) flush() {
	p.mu.Lock()
	p.flushLocked()
	p.mu.Unlock()
}

func (p *QueryPipeline) flushLocked() {
	if len(p.buffer) == 0 {
		return
	}

	// Process batch
	queries := make([]*PipelineQuery, len(p.buffer))
	copy(queries, p.buffer)
	p.buffer = p.buffer[:0]

	// Reset timer
	if p.flushTimer != nil {
		p.flushTimer.Stop()
		p.flushTimer = nil
	}

	// Submit batch job to worker pool
	job := &BatchJob{
		Queries:  queries,
		Pipeline: p,
	}
	
	select {
	case p.workerPool.jobQueue <- job:
		// Job submitted successfully
	default:
		// Worker pool is busy, process synchronously
		go p.processBatch(queries)
	}
	
	atomic.AddUint64(&p.stats.BatchesProcessed, 1)
}

// processBatch executes a batch of queries concurrently with optimized resource usage
func (p *QueryPipeline) processBatch(queries []*PipelineQuery) {
	// Use a semaphore to limit concurrent executions within the batch
	// This prevents resource exhaustion when processing large batches
	semaphore := make(chan struct{}, runtime.NumCPU())
	var wg sync.WaitGroup
	wg.Add(len(queries))

	for _, q := range queries {
		semaphore <- struct{}{} // Acquire semaphore
		go func(query *PipelineQuery) {
			defer func() {
				<-semaphore // Release semaphore
				wg.Done()
			}()

			execStart := time.Now()
			result, err := p.executor.Execute(query.Context, query.Query)
			duration := time.Since(execStart)
			
			query.Result <- &PipelineResult{
				ResultSet: result,
				Error:     err,
				Duration:  duration,
			}
		}(q)
	}

	wg.Wait()
	
	// Update batch statistics
	atomic.AddUint64(&p.stats.BatchesProcessed, 1)
	
	// Update average batch size (simplified moving average)
	currentAvg := atomic.LoadUint64((*uint64)(&p.stats.AverageBatchSize))
	newAvg := (currentAvg + uint64(len(queries))) / 2
	atomic.StoreUint64((*uint64)(&p.stats.AverageBatchSize), newAvg)
}

// adjustBatchSizeLocked dynamically adjusts the batch size based on current queue conditions
// This method should only be called when holding the mutex lock
//
// The adaptive batching strategy works as follows:
// 1. Monitor queue utilization over time
// 2. When queue utilization is high (>80% by default), increase batch size to process more queries at once
// 3. When queue utilization is low (<20% by default), decrease batch size to reduce latency for individual queries
// 4. Adjustments are rate-limited to prevent rapid oscillations
//
// This approach provides several benefits:
// - Under high load: Larger batches improve throughput by reducing per-query overhead
// - Under low load: Smaller batches reduce latency for better responsiveness
// - Automatic adaptation: The system responds to workload patterns without manual tuning
func (p *QueryPipeline) adjustBatchSizeLocked() {
	// Only adjust if dynamic batching is enabled and enough time has passed since last adjustment
	if !p.config.EnableDynamicBatching || time.Since(p.lastAdjustment) < p.config.AdjustmentInterval {
		return
	}
	
	// Calculate current queue utilization
	queueUtilization := float64(len(p.buffer)) / float64(p.maxQueueSize)
	
	// Adjust batch size based on queue utilization
	if queueUtilization > p.config.QueueGrowthThreshold {
		// Queue is getting full, increase batch size to process more queries at once
		newBatchSize := int(float64(p.batchSize) * p.config.BatchSizeIncreaseFactor)
		if newBatchSize > p.config.MaxBatchSize {
			newBatchSize = p.config.MaxBatchSize
		}
		if newBatchSize != p.batchSize {
			p.batchSize = newBatchSize
			// Update max queue size proportionally
			p.maxQueueSize = p.batchSize * 4
			// Ensure buffer has adequate capacity
			if cap(p.buffer) < p.maxQueueSize {
				newBuffer := make([]*PipelineQuery, len(p.buffer), p.maxQueueSize)
				copy(newBuffer, p.buffer)
				p.buffer = newBuffer
			}
		}
	} else if queueUtilization < p.config.QueueShrinkThreshold {
		// Queue is relatively empty, decrease batch size to reduce latency
		newBatchSize := int(float64(p.batchSize) * p.config.BatchSizeDecreaseFactor)
		if newBatchSize < p.config.MinBatchSize {
			newBatchSize = p.config.MinBatchSize
		}
		if newBatchSize != p.batchSize {
			p.batchSize = newBatchSize
			// Update max queue size proportionally
			p.maxQueueSize = p.batchSize * 4
		}
	}
	
	// Update current batch size in stats
	atomic.StoreInt64(&p.stats.CurrentBatchSize, int64(p.batchSize))
	
	// Record the time of this adjustment
	p.lastAdjustment = time.Now()
}

// worker processes batch jobs from the worker pool
func (wp *WorkerPool) worker() {
	defer wp.wg.Done()
	
	for {
		select {
		case job := <-wp.jobQueue:
			atomic.AddInt64(&job.Pipeline.stats.WorkersActive, 1)
			job.Pipeline.processBatch(job.Queries)
			atomic.AddInt64(&job.Pipeline.stats.WorkersActive, -1)
		case <-wp.ctx.Done():
			return
		}
	}
}

// Close shuts down the pipeline and worker pool gracefully
func (p *QueryPipeline) Close() {
	if p.workerPool != nil {
		p.workerPool.cancel()
		close(p.workerPool.jobQueue)
		p.workerPool.wg.Wait()
	}
	
	if p.flushTimer != nil {
		p.flushTimer.Stop()
	}
}

// Stats returns current pipeline statistics
func (p *QueryPipeline) Stats() PipelineStats {
	return PipelineStats{
		QueriesProcessed: atomic.LoadUint64(&p.stats.QueriesProcessed),
		BatchesProcessed: atomic.LoadUint64(&p.stats.BatchesProcessed),
		AverageBatchSize: atomic.LoadUint64(&p.stats.AverageBatchSize),
		QueueLength:      atomic.LoadInt64(&p.stats.QueueLength),
		WorkersActive:    atomic.LoadInt64(&p.stats.WorkersActive),
		CurrentBatchSize: atomic.LoadInt64(&p.stats.CurrentBatchSize),
	}
}

// UpdateConfig updates the pipeline configuration at runtime
//
// This method allows dynamic reconfiguration of the pipeline without restarting:
// - Preserves existing buffered queries
// - Applies new configuration parameters immediately
// - Resets batch size to base value to allow fresh adaptation
// - Maintains backward compatibility with existing interfaces
func (p *QueryPipeline) UpdateConfig(config PipelineConfig) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Apply defaults for missing configuration values
	defaultConfig := DefaultPipelineConfig()
	if config.MinBatchSize <= 0 {
		config.MinBatchSize = defaultConfig.MinBatchSize
	}
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = defaultConfig.MaxBatchSize
	}
	if config.AdjustmentInterval <= 0 {
		config.AdjustmentInterval = defaultConfig.AdjustmentInterval
	}
	if config.QueueGrowthThreshold <= 0 {
		config.QueueGrowthThreshold = defaultConfig.QueueGrowthThreshold
	}
	if config.QueueShrinkThreshold <= 0 {
		config.QueueShrinkThreshold = defaultConfig.QueueShrinkThreshold
	}
	if config.BatchSizeIncreaseFactor <= 1.0 {
		config.BatchSizeIncreaseFactor = defaultConfig.BatchSizeIncreaseFactor
	}
	if config.BatchSizeDecreaseFactor >= 1.0 || config.BatchSizeDecreaseFactor <= 0 {
		config.BatchSizeDecreaseFactor = defaultConfig.BatchSizeDecreaseFactor
	}
	
	p.config = config
	
	// Reset batch size to base value and let dynamic adjustment take over
	p.batchSize = config.BaseBatchSize
	p.maxQueueSize = p.batchSize * 4
	
	// Ensure buffer has adequate capacity
	if cap(p.buffer) < p.maxQueueSize {
		newBuffer := make([]*PipelineQuery, len(p.buffer), p.maxQueueSize)
		copy(newBuffer, p.buffer)
		p.buffer = newBuffer
	}
}