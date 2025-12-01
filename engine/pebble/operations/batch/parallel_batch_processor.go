package batch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// ParallelBatchProcessorConfig holds configuration for parallel batch processing
type ParallelBatchProcessorConfig struct {
	*BatchProcessorConfig
	MaxConcurrency int // Maximum number of concurrent operations
	WorkerPoolSize int // Size of the worker pool for parallel processing
}

// DefaultParallelBatchProcessorConfig returns the default parallel batch processor configuration
func DefaultParallelBatchProcessorConfig() *ParallelBatchProcessorConfig {
	return &ParallelBatchProcessorConfig{
		BatchProcessorConfig: DefaultBatchProcessorConfig(),
		MaxConcurrency:       8,  // Default to 8 concurrent operations
		WorkerPoolSize:       16, // Default to 16 worker threads
	}
}

// ParallelBatchProcessorImpl implements parallel batch processing operations
type ParallelBatchProcessorImpl struct {
	kv     storage.KV
	codec  codec.Codec
	config *ParallelBatchProcessorConfig
	stats  *BatchProcessorStats
	
	// Worker pool for parallel processing
	workerPool chan chan workItem
	workers    []*worker
	wg         sync.WaitGroup
}

// workItem represents a unit of work for the worker pool
type workItem struct {
	ctx       context.Context
	tenantID  int64
	tableID   int64
	rows      []*dbTypes.Record
	schemaDef *dbTypes.TableDefinition
	resultCh  chan workResult
}

// workResult represents the result of a work item
type workResult struct {
	rowIDs []int64
	err    error
}

// worker represents a worker in the worker pool
type worker struct {
	id         int
	workChan   chan workItem
	processor  *BatchProcessorImpl // Use existing batch processor for individual work items
	quitChan   chan bool
	doneChan   chan bool // Channel to signal when worker is done
}

// newWorker creates a new worker
func newWorker(id int, kv storage.KV, codec codec.Codec, config *BatchProcessorConfig) *worker {
	return &worker{
		id:        id,
		workChan:  make(chan workItem),
		processor: NewBatchProcessorWithConfig(kv, codec, config),
		quitChan:  make(chan bool),
		doneChan:  make(chan bool),
	}
}

// start begins the worker's processing loop
func (w *worker) start() {
	go func() {
		defer func() {
			// Signal that worker is done
			close(w.doneChan)
		}()
		
		for {
			select {
			case work := <-w.workChan:
				// Process the work item
				rowIDs, err := w.processor.ProcessBatchInsert(work.ctx, work.tenantID, work.tableID, work.rows, work.schemaDef)
				
				// Send result back
				work.resultCh <- workResult{
					rowIDs: rowIDs,
					err:    err,
				}
			case <-w.quitChan:
				return
			}
		}
	}()
}

// stop signals the worker to stop and waits for it to finish
func (w *worker) stop() {
	// Check if already stopped to prevent panic
	select {
	case <-w.quitChan:
		// Already stopped, return immediately
		return
	default:
		// Not stopped yet, proceed with stopping
		close(w.quitChan)
	}
	
	// Wait for worker to finish (with timeout)
	select {
	case <-w.doneChan:
		// Worker finished normally
	case <-time.After(1 * time.Second):
		// Timeout - worker didn't finish in time
		// In a real implementation, you might want to log this
	}
}

// NewParallelBatchProcessor creates a new parallel batch processor
func NewParallelBatchProcessor(kv storage.KV, codec codec.Codec) *ParallelBatchProcessorImpl {
	return NewParallelBatchProcessorWithConfig(kv, codec, DefaultParallelBatchProcessorConfig())
}

// NewParallelBatchProcessorWithConfig creates a new parallel batch processor with custom configuration
func NewParallelBatchProcessorWithConfig(kv storage.KV, codec codec.Codec, config *ParallelBatchProcessorConfig) *ParallelBatchProcessorImpl {
	if config.WorkerPoolSize <= 0 {
		config.WorkerPoolSize = 16
	}
	
	if config.MaxConcurrency <= 0 {
		config.MaxConcurrency = 8
	}
	
	pbp := &ParallelBatchProcessorImpl{
		kv:     kv,
		codec:  codec,
		config: config,
		stats:  &BatchProcessorStats{},
		workerPool: make(chan chan workItem, config.WorkerPoolSize),
		workers:    make([]*worker, config.WorkerPoolSize),
	}
	
	// Start worker pool
	for i := 0; i < config.WorkerPoolSize; i++ {
		worker := newWorker(i, kv, codec, config.BatchProcessorConfig)
		worker.start()
		pbp.workers[i] = worker
		pbp.workerPool <- worker.workChan
	}
	
	return pbp
}

// ProcessBatchInsert processes a batch insert operation with parallel processing
func (pbp *ParallelBatchProcessorImpl) ProcessBatchInsert(ctx context.Context, tenantID, tableID int64, rows []*dbTypes.Record, schemaDef *dbTypes.TableDefinition) ([]int64, error) {
	if len(rows) == 0 {
		return []int64{}, nil
	}
	
	// For small batches, use sequential processing to avoid overhead
	if len(rows) < pbp.config.MinBatchSize*2 {
		processor := NewBatchProcessorWithConfig(pbp.kv, pbp.codec, pbp.config.BatchProcessorConfig)
		return processor.ProcessBatchInsert(ctx, tenantID, tableID, rows, schemaDef)
	}
	
	// Split large batches into chunks for parallel processing
	chunkSize := pbp.getOptimalChunkSize(len(rows))
	concurrency := min(pbp.config.MaxConcurrency, (len(rows)+chunkSize-1)/chunkSize)
	
	// Create channels for work distribution and results
	workItems := make([]workItem, 0, concurrency)
	resultChans := make([]chan workResult, 0, concurrency)
	
	// Split rows into chunks
	for i := 0; i < len(rows); i += chunkSize {
		end := i + chunkSize
		if end > len(rows) {
			end = len(rows)
		}
		
		resultCh := make(chan workResult, 1)
		workItem := workItem{
			ctx:       ctx,
			tenantID:  tenantID,
			tableID:   tableID,
			rows:      rows[i:end],
			schemaDef: schemaDef,
			resultCh:  resultCh,
		}
		
		workItems = append(workItems, workItem)
		resultChans = append(resultChans, resultCh)
	}
	
	// Distribute work to worker pool
	var wg sync.WaitGroup
	errChan := make(chan error, len(workItems))
	
	for _, work := range workItems {
		wg.Add(1)
		go func(w workItem) {
			defer wg.Done()
			
			// Get an available worker
			select {
			case workChan := <-pbp.workerPool:
				// Send work to worker
				workChan <- w
				
				// Return worker to pool
				pbp.workerPool <- workChan
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}(work)
	}
	
	// Wait for all work to be distributed
	wg.Wait()
	close(errChan)
	
	// Check for context cancellation errors
	if err := <-errChan; err != nil {
		return nil, err
	}
	
	// Collect results
	allRowIDs := make([]int64, 0, len(rows))
	for _, resultCh := range resultChans {
		result := <-resultCh
		if result.err != nil {
			return nil, result.err
		}
		allRowIDs = append(allRowIDs, result.rowIDs...)
	}
	
	// Update statistics
	atomic.AddInt64(&pbp.stats.TotalBatchesProcessed, int64(len(workItems)))
	atomic.AddInt64(&pbp.stats.TotalRowsProcessed, int64(len(rows)))
	
	return allRowIDs, nil
}

// ProcessBatchUpdate processes a batch update operation
func (pbp *ParallelBatchProcessorImpl) ProcessBatchUpdate(ctx context.Context, tenantID, tableID int64, updates []engineTypes.RowUpdate, schemaDef *dbTypes.TableDefinition) error {
	// For now, delegate to the sequential processor
	// Future optimization could parallelize updates by splitting into chunks
	processor := NewBatchProcessorWithConfig(pbp.kv, pbp.codec, pbp.config.BatchProcessorConfig)
	return processor.ProcessBatchUpdate(ctx, tenantID, tableID, updates, schemaDef)
}

// ProcessBatchDelete processes a batch delete operation
func (pbp *ParallelBatchProcessorImpl) ProcessBatchDelete(ctx context.Context, tenantID, tableID int64, rowIDs []int64, schemaDef *dbTypes.TableDefinition) error {
	// For now, delegate to the sequential processor
	// Future optimization could parallelize deletes by splitting into chunks
	processor := NewBatchProcessorWithConfig(pbp.kv, pbp.codec, pbp.config.BatchProcessorConfig)
	return processor.ProcessBatchDelete(ctx, tenantID, tableID, rowIDs, schemaDef)
}

// getOptimalChunkSize determines the optimal chunk size based on batch size and configuration
func (pbp *ParallelBatchProcessorImpl) getOptimalChunkSize(batchSize int) int {
	// Use the target batch size as chunk size for optimal performance
	chunkSize := pbp.config.TargetBatchSize
	
	// Ensure chunk size is reasonable
	if chunkSize < pbp.config.MinBatchSize {
		chunkSize = pbp.config.MinBatchSize
	}
	if chunkSize > pbp.config.MaxBatchSize {
		chunkSize = pbp.config.MaxBatchSize
	}
	
	// Adjust for very large batches to maintain reasonable concurrency
	if batchSize/chunkSize > pbp.config.MaxConcurrency*2 {
		chunkSize = batchSize / pbp.config.MaxConcurrency
	}
	
	return chunkSize
}

// Close shuts down the parallel batch processor and its worker pool
func (pbp *ParallelBatchProcessorImpl) Close() error {
	// Stop all workers
	for _, worker := range pbp.workers {
		worker.stop()
	}
	
	return nil
}

// GetStats returns the current batch processor statistics
func (pbp *ParallelBatchProcessorImpl) GetStats() *BatchProcessorStats {
	return &BatchProcessorStats{
		TotalBatchesProcessed: atomic.LoadInt64(&pbp.stats.TotalBatchesProcessed),
		TotalRowsProcessed:    atomic.LoadInt64(&pbp.stats.TotalRowsProcessed),
		AverageBatchSize:      pbp.calculateAverageBatchSize(),
		QueueLength:           atomic.LoadInt64(&pbp.stats.QueueLength),
	}
}

// calculateAverageBatchSize calculates the average batch size
func (pbp *ParallelBatchProcessorImpl) calculateAverageBatchSize() float64 {
	totalBatches := atomic.LoadInt64(&pbp.stats.TotalBatchesProcessed)
	totalRows := atomic.LoadInt64(&pbp.stats.TotalRowsProcessed)
	
	if totalBatches == 0 {
		return 0
	}
	
	return float64(totalRows) / float64(totalBatches)
}