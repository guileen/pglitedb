// Advanced query execution pipeline with optimized batch processing and resource management
package sql

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
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
}

// PipelineStats contains statistics about the pipeline performance
type PipelineStats struct {
	QueriesProcessed uint64
	BatchesProcessed uint64
	AverageBatchSize float64
	QueueLength      int64
	WorkersActive    int64
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
	ResultSet *ResultSet
	Error     error
	Duration  time.Duration
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
	if batchSize <= 0 {
		batchSize = 10
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
	
	return &QueryPipeline{
		executor:     executor,
		batchSize:    batchSize,
		maxQueueSize: batchSize * 4, // Allow up to 4 batches in queue
		buffer:       make([]*PipelineQuery, 0, batchSize*2),
		workerPool:   wp,
	}
}

// Execute adds a query to the pipeline and returns the result
func (p *QueryPipeline) Execute(ctx context.Context, query string) (*ResultSet, error) {
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
	startTime := time.Now()
	
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
	batchDuration := time.Since(startTime)
	atomic.AddUint64(&p.stats.BatchesProcessed, 1)
	
	// Update average batch size (simplified moving average)
	currentAvg := atomic.LoadUint64((*uint64)(&p.stats.AverageBatchSize))
	newAvg := (currentAvg + uint64(len(queries))) / 2
	atomic.StoreUint64((*uint64)(&p.stats.AverageBatchSize), newAvg)
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
		AverageBatchSize: float64(atomic.LoadUint64((*uint64)(&p.stats.AverageBatchSize))),
		QueueLength:      atomic.LoadInt64(&p.stats.QueueLength),
		WorkersActive:    atomic.LoadInt64(&p.stats.WorkersActive),
	}

}