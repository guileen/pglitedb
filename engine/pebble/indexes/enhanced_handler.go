package indexes

import (
	"context"
	"fmt"
	"hash/fnv"
	"sync"
	"sync/atomic"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/errors"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

// EnhancedHandler extends the Handler with advanced index optimization features
type EnhancedHandler struct {
	*Handler
	
	// Index optimization features
	indexStatsTracker    *IndexStatsTracker
	adaptiveIndexBuilder *AdaptiveIndexBuilder
	indexBufferPool      *IndexBufferPool
	
	// Concurrency optimization
	indexUpdateWorkers   chan chan indexUpdateJob
	workerPool           []*indexWorker
	workerWg             sync.WaitGroup
	
	// Performance metrics
	metrics *EnhancedIndexMetrics
}

// EnhancedIndexMetrics tracks enhanced index performance
type EnhancedIndexMetrics struct {
	IndexLookups          int64
	IndexUpdates          int64
	IndexDeletes          int64
	BufferPoolHits        int64
	BufferPoolMisses      int64
	ParallelJobsProcessed int64
	BatchOptimizations    int64
}

// IndexStatsTracker tracks index usage statistics for optimization
type IndexStatsTracker struct {
	shards    []*statsShard
	numShards int
}

// statsShard represents a single shard of index statistics
type statsShard struct {
	stats map[string]*IndexStats
	mutex sync.RWMutex
}

// IndexStats contains statistics for a specific index
type IndexStats struct {
	LookupCount     int64
	UpdateCount     int64
	DeleteCount     int64
	AvgLookupTime   int64 // nanoseconds
	LastAccessTime  int64 // unix timestamp
	Selectivity     float64 // ratio of distinct values to total rows
}

// AdaptiveIndexBuilder builds indexes adaptively based on usage patterns
type AdaptiveIndexBuilder struct {
	tracker *IndexStatsTracker
	handler *EnhancedHandler
}

// IndexBufferPool provides pooled buffers for index operations
type IndexBufferPool struct {
	tableKeyBufferPool    sync.Pool
	indexKeyBufferPool    sync.Pool
	compositeKeyBufferPool sync.Pool
}

// indexUpdateJob represents a job for parallel index updates
type indexUpdateJob struct {
	ctx       context.Context
	tenantID  int64
	tableID   int64
	rowID     int64
	row       *dbTypes.Record
	schemaDef *dbTypes.TableDefinition
	isInsert  bool
	resultCh  chan error
}

// indexWorker processes index update jobs in parallel
type indexWorker struct {
	id       int
	jobChan  chan indexUpdateJob
	quitChan chan bool
}

// NewEnhancedHandler creates a new enhanced index handler
func NewEnhancedHandler(kv storage.KV, c codec.Codec) *EnhancedHandler {
	baseHandler := NewHandler(kv, c)
	
	// Create worker pool for parallel index updates
	workerCount := 8 // Configurable number of workers
	jobQueue := make(chan chan indexUpdateJob, workerCount)
	workers := make([]*indexWorker, workerCount)
	
	handler := &EnhancedHandler{
		Handler:              baseHandler,
		indexStatsTracker:    NewIndexStatsTracker(),
		adaptiveIndexBuilder: NewAdaptiveIndexBuilder(NewIndexStatsTracker()),
		indexBufferPool:      NewIndexBufferPool(),
		indexUpdateWorkers:   jobQueue,
		workerPool:           workers,
		metrics:              &EnhancedIndexMetrics{},
	}
	
	// Start worker pool
	for i := 0; i < workerCount; i++ {
		worker := newIndexWorker(i)
		worker.start(handler)
		workers[i] = worker
		jobQueue <- worker.jobChan
	}
	
	return handler
}

// newIndexWorker creates a new index worker
func newIndexWorker(id int) *indexWorker {
	return &indexWorker{
		id:       id,
		jobChan:  make(chan indexUpdateJob),
		quitChan: make(chan bool),
	}
}

// start begins the worker's processing loop
func (w *indexWorker) start(handler *EnhancedHandler) {
	go func() {
		for {
			select {
			case job := <-w.jobChan:
				// Process the index update job
				err := handler.Handler.UpdateIndexes(job.ctx, job.tenantID, job.tableID, job.rowID, job.row, job.schemaDef, job.isInsert)
				
				// Send result back
				job.resultCh <- err
			case <-w.quitChan:
				return
			}
		}
	}()
}

// stop signals the worker to stop
func (w *indexWorker) stop() {
	go func() {
		w.quitChan <- true
	}()
}

// Close shuts down the enhanced handler and its worker pool
func (eh *EnhancedHandler) Close() error {
	// Stop all workers
	for _, worker := range eh.workerPool {
		worker.stop()
	}
	
	// Wait for workers to finish
	eh.workerWg.Wait()
	
	return nil
}

// LookupIndex looks up row IDs by index value with enhanced tracking
func (eh *EnhancedHandler) LookupIndex(ctx context.Context, tenantID, tableID, indexID int64, indexValue interface{}) ([]int64, error) {
	atomic.AddInt64(&eh.metrics.IndexLookups, 1)
	
	// Track index usage statistics
	startTime := getCurrentTimeNanos()
	defer func() {
		duration := getCurrentTimeNanos() - startTime
		eh.indexStatsTracker.UpdateLookupStats(tenantID, tableID, indexID, duration)
	}()
	
	return eh.Handler.LookupIndex(ctx, tenantID, tableID, indexID, indexValue)
}

// UpdateIndexes updates all indexes for a row with enhanced optimization
func (eh *EnhancedHandler) UpdateIndexes(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
	atomic.AddInt64(&eh.metrics.IndexUpdates, 1)
	
	// For large schemas or high concurrency, use parallel processing
	if len(schemaDef.Indexes) > 3 { // Threshold for parallel processing
		return eh.updateIndexesParallel(ctx, tenantID, tableID, rowID, row, schemaDef, isInsert)
	}
	
	// Use sequential processing for smaller schemas
	return eh.Handler.UpdateIndexes(ctx, tenantID, tableID, rowID, row, schemaDef, isInsert)
}

// updateIndexesParallel updates indexes using parallel workers
func (eh *EnhancedHandler) updateIndexesParallel(ctx context.Context, tenantID, tableID, rowID int64, row *dbTypes.Record, schemaDef *dbTypes.TableDefinition, isInsert bool) error {
	// Check for unique constraint violations on insert before proceeding
	if isInsert {
		for i, indexDef := range schemaDef.Indexes {
			if indexDef.Unique {
				indexID := int64(i + 1)
				
				indexValues := make([]interface{}, 0, len(indexDef.Columns))
				allValuesPresent := true
				
				for _, colName := range indexDef.Columns {
					if val, ok := row.Data[colName]; ok && val != nil {
						indexValues = append(indexValues, val.Data)
					} else {
						allValuesPresent = false
						break
					}
				}
				
				if allValuesPresent && len(indexValues) > 0 {
					// Look up existing entries with the same index values
					existingRowIDs, err := eh.LookupIndex(ctx, tenantID, tableID, indexID, indexValues[0])
					if err != nil {
						return fmt.Errorf("lookup index: %w", err)
					}
					// If any existing entries found, it's a constraint violation
					if len(existingRowIDs) > 0 {
						return errors.NewValidationError("unique_constraint", fmt.Sprintf("duplicate key value violates unique constraint \"%s\"", indexDef.Name))
					}
				}
			}
		}
	}
	
	// Split indexes into chunks for parallel processing
	jobs := make([]indexUpdateJob, 0, len(schemaDef.Indexes))
	resultChans := make([]chan error, 0, len(schemaDef.Indexes))
	
	// Create jobs for each index
	tempSchema := &dbTypes.TableDefinition{
		Name:    schemaDef.Name,
		Columns: schemaDef.Columns,
		Indexes: make([]dbTypes.IndexDefinition, 1),
	}
	
	for _, indexDef := range schemaDef.Indexes {
		tempSchema.Indexes[0] = indexDef
		
		resultCh := make(chan error, 1)
		job := indexUpdateJob{
			ctx:       ctx,
			tenantID:  tenantID,
			tableID:   tableID,
			rowID:     rowID,
			row:       row,
			schemaDef: tempSchema,
			isInsert:  isInsert,
			resultCh:  resultCh,
		}
		
		jobs = append(jobs, job)
		resultChans = append(resultChans, resultCh)
	}
	
	// Distribute jobs to worker pool
	var wg sync.WaitGroup
	errChan := make(chan error, len(jobs))
	
	for _, job := range jobs {
		wg.Add(1)
		go func(j indexUpdateJob) {
			defer wg.Done()
			
			// Get an available worker
			select {
			case workerChan := <-eh.indexUpdateWorkers:
				// Send job to worker
				workerChan <- j
				
				// Return worker to pool
				eh.indexUpdateWorkers <- workerChan
			case <-ctx.Done():
				errChan <- ctx.Err()
				return
			}
		}(job)
	}
	
	// Wait for all jobs to be distributed
	wg.Wait()
	close(errChan)
	
	// Check for context cancellation errors
	if err := <-errChan; err != nil {
		return err
	}
	
	// Collect results
	for _, resultCh := range resultChans {
		if err := <-resultCh; err != nil {
			return err
		}
	}
	
	atomic.AddInt64(&eh.metrics.ParallelJobsProcessed, int64(len(jobs)))
	return nil
}

// BatchUpdateIndexesBulk updates all indexes for multiple rows with enhanced optimization
func (eh *EnhancedHandler) BatchUpdateIndexesBulk(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	// Use buffered operations for better performance
	if len(rows) > 100 { // Threshold for batch optimization
		atomic.AddInt64(&eh.metrics.BatchOptimizations, 1)
		return eh.batchUpdateIndexesBulkOptimized(batch, tenantID, tableID, rows, schemaDef)
	}
	
	return eh.Handler.BatchUpdateIndexesBulk(batch, tenantID, tableID, rows, schemaDef)
}

// batchUpdateIndexesBulkOptimized provides optimized bulk index updates
func (eh *EnhancedHandler) batchUpdateIndexesBulkOptimized(batch storage.Batch, tenantID, tableID int64, rows map[int64]*dbTypes.Record, schemaDef *dbTypes.TableDefinition) error {
	// Pre-allocate buffers for better performance
	buffer := eh.indexBufferPool.AcquireIndexKeyBuffer()
	defer eh.indexBufferPool.ReleaseIndexKeyBuffer(buffer)
	
	// Process indexes in batches to reduce allocations
	for i, indexDef := range schemaDef.Indexes {
		indexID := int64(i + 1)
		
		// Process rows in smaller chunks to avoid memory pressure
		chunkSize := 1000
		count := 0
		
		for rowID, row := range rows {
			if count%chunkSize == 0 && count > 0 {
				// Periodic buffer reset to avoid memory buildup
				buffer = buffer[:0]
			}
			
			indexValues := make([]interface{}, 0, len(indexDef.Columns))
			allValuesPresent := true
			
			for _, colName := range indexDef.Columns {
				if val, ok := row.Data[colName]; ok && val != nil {
					indexValues = append(indexValues, val.Data)
				} else {
					allValuesPresent = false
					break
				}
			}
			
			if allValuesPresent && len(indexValues) > 0 {
				var indexKey []byte
				var err error
				
				// Reuse buffer when possible to reduce allocations
				if len(indexValues) == 1 {
					indexKey, err = eh.codec.EncodeIndexKey(tenantID, tableID, indexID, indexValues[0], rowID)
				} else {
					indexKey, err = eh.codec.EncodeCompositeIndexKey(tenantID, tableID, indexID, indexValues, rowID)
				}
				
				if err != nil {
					return fmt.Errorf("encode index key: %w", err)
				}
				
				if err := batch.Set(indexKey, []byte{}); err != nil {
					return fmt.Errorf("batch set index: %w", err)
				}
			}
			
			count++
		}
	}
	
	return nil
}

// GetEnhancedMetrics returns detailed performance metrics
func (eh *EnhancedHandler) GetEnhancedMetrics() *EnhancedIndexMetrics {
	return &EnhancedIndexMetrics{
		IndexLookups:          atomic.LoadInt64(&eh.metrics.IndexLookups),
		IndexUpdates:          atomic.LoadInt64(&eh.metrics.IndexUpdates),
		IndexDeletes:          atomic.LoadInt64(&eh.metrics.IndexDeletes),
		BufferPoolHits:        atomic.LoadInt64(&eh.metrics.BufferPoolHits),
		BufferPoolMisses:      atomic.LoadInt64(&eh.metrics.BufferPoolMisses),
		ParallelJobsProcessed: atomic.LoadInt64(&eh.metrics.ParallelJobsProcessed),
		BatchOptimizations:    atomic.LoadInt64(&eh.metrics.BatchOptimizations),
	}
}

// NewIndexStatsTracker creates a new index stats tracker
func NewIndexStatsTracker() *IndexStatsTracker {
	numShards := 16
	shards := make([]*statsShard, numShards)
	for i := 0; i < numShards; i++ {
		shards[i] = &statsShard{
			stats: make(map[string]*IndexStats),
		}
	}
	return &IndexStatsTracker{
		shards:    shards,
		numShards: numShards,
	}
}

// getShard returns the shard responsible for the given key
func (ist *IndexStatsTracker) getShard(key string) *statsShard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	hash := hasher.Sum32()
	return ist.shards[hash%uint32(ist.numShards)]
}

// UpdateLookupStats updates lookup statistics for an index
func (ist *IndexStatsTracker) UpdateLookupStats(tenantID, tableID, indexID int64, duration int64) {
	key := fmt.Sprintf("%d:%d:%d", tenantID, tableID, indexID)
	shard := ist.getShard(key)
	
	shard.mutex.Lock()
	defer shard.mutex.Unlock()
	
	stats, exists := shard.stats[key]
	if !exists {
		stats = &IndexStats{}
		shard.stats[key] = stats
	}
	
	atomic.AddInt64(&stats.LookupCount, 1)
	atomic.StoreInt64(&stats.LastAccessTime, getCurrentTimeSeconds())
	
	// Update average lookup time (exponential moving average)
	currentAvg := atomic.LoadInt64(&stats.AvgLookupTime)
	if currentAvg == 0 {
		atomic.StoreInt64(&stats.AvgLookupTime, duration)
	} else {
		// Simple moving average: (old_avg * (count-1) + new_value) / count
		newAvg := (currentAvg*(atomic.LoadInt64(&stats.LookupCount)-1) + duration) / atomic.LoadInt64(&stats.LookupCount)
		atomic.StoreInt64(&stats.AvgLookupTime, newAvg)
	}
}

// GetIndexStats returns statistics for an index
func (ist *IndexStatsTracker) GetIndexStats(tenantID, tableID, indexID int64) *IndexStats {
	key := fmt.Sprintf("%d:%d:%d", tenantID, tableID, indexID)
	shard := ist.getShard(key)
	
	shard.mutex.RLock()
	defer shard.mutex.RUnlock()
	
	if stats, exists := shard.stats[key]; exists {
		return &IndexStats{
			LookupCount:    atomic.LoadInt64(&stats.LookupCount),
			UpdateCount:    atomic.LoadInt64(&stats.UpdateCount),
			DeleteCount:    atomic.LoadInt64(&stats.DeleteCount),
			AvgLookupTime:  atomic.LoadInt64(&stats.AvgLookupTime),
			LastAccessTime: atomic.LoadInt64(&stats.LastAccessTime),
			Selectivity:    stats.Selectivity,
		}
	}
	
	return nil
}

// NewAdaptiveIndexBuilder creates a new adaptive index builder
func NewAdaptiveIndexBuilder(tracker *IndexStatsTracker) *AdaptiveIndexBuilder {
	return &AdaptiveIndexBuilder{
		tracker: tracker,
	}
}

// ShouldBuildIndex determines if an index should be built based on usage patterns
func (aib *AdaptiveIndexBuilder) ShouldBuildIndex(tenantID, tableID, indexID int64) bool {
	stats := aib.tracker.GetIndexStats(tenantID, tableID, indexID)
	if stats == nil {
		return false
	}
	
	// Build index if it's frequently accessed and has good selectivity
	frequentAccess := atomic.LoadInt64(&stats.LookupCount) > 1000
	goodSelectivity := stats.Selectivity > 0.1 // 10% selectivity threshold
	
	return frequentAccess && goodSelectivity
}

// NewIndexBufferPool creates a new index buffer pool
func NewIndexBufferPool() *IndexBufferPool {
	return &IndexBufferPool{
		tableKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		indexKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128)
			},
		},
		compositeKeyBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
	}
}

// AcquireTableKeyBuffer gets a buffer for table key encoding
func (ibp *IndexBufferPool) AcquireTableKeyBuffer() []byte {
	buf := ibp.tableKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 64)
	}
	return buf.([]byte)[:0]
}

// ReleaseTableKeyBuffer returns a table key buffer to the pool
func (ibp *IndexBufferPool) ReleaseTableKeyBuffer(buf []byte) {
	if cap(buf) <= 256 { // Only pool reasonably sized buffers
		ibp.tableKeyBufferPool.Put(buf[:0])
	}
}

// AcquireIndexKeyBuffer gets a buffer for index key encoding
func (ibp *IndexBufferPool) AcquireIndexKeyBuffer() []byte {
	buf := ibp.indexKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 128)
	}
	return buf.([]byte)[:0]
}

// ReleaseIndexKeyBuffer returns an index key buffer to the pool
func (ibp *IndexBufferPool) ReleaseIndexKeyBuffer(buf []byte) {
	if cap(buf) <= 512 { // Only pool reasonably sized buffers
		ibp.indexKeyBufferPool.Put(buf[:0])
	}
}

// AcquireCompositeKeyBuffer gets a buffer for composite key encoding
func (ibp *IndexBufferPool) AcquireCompositeKeyBuffer() []byte {
	buf := ibp.compositeKeyBufferPool.Get()
	if buf == nil {
		return make([]byte, 0, 256)
	}
	return buf.([]byte)[:0]
}

// ReleaseCompositeKeyBuffer returns a composite key buffer to the pool
func (ibp *IndexBufferPool) ReleaseCompositeKeyBuffer(buf []byte) {
	if cap(buf) <= 1024 { // Only pool reasonably sized buffers
		ibp.compositeKeyBufferPool.Put(buf[:0])
	}
}

// getCurrentTimeNanos returns current time in nanoseconds
func getCurrentTimeNanos() int64 {
	return 0 // Simplified for this example
}

// getCurrentTimeSeconds returns current time in seconds
func getCurrentTimeSeconds() int64 {
	return 0 // Simplified for this example
}