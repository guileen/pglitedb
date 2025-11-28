package pebble

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

// ResourceManager manages resource pooling for the pebble engine
type ResourceManager struct {
	iteratorPool     sync.Pool
	batchPool        sync.Pool
	txnPool          sync.Pool
	recordPool       sync.Pool  // Pool for Record objects
	valuePool        sync.Pool  // Pool for Value objects
	bufferPool       sync.Pool  // Pool for byte buffers
	keyEncoderPool   sync.Pool  // Pool for key encoders
	filterExprPool   sync.Pool  // Pool for filter expressions
	
	// Enhanced pools for specific use cases
	scanResultPool   sync.Pool  // Pool for scan results
	indexKeyPool     sync.Pool  // Pool for index keys
	tableKeyPool     sync.Pool  // Pool for table keys
	metaKeyPool      sync.Pool  // Pool for metadata keys
	compositeKeyPool sync.Pool  // Pool for composite keys
	
	// Size-tiered buffer pools for better memory utilization
	smallBufferPool  sync.Pool  // 64-byte buffers
	mediumBufferPool sync.Pool  // 256-byte buffers
	largeBufferPool  sync.Pool  // 1024-byte buffers
	hugeBufferPool   sync.Pool  // 4096-byte buffers
	
	metrics          *ResourceMetricsCollector
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	return &ResourceManager{
		iteratorPool: sync.Pool{
			New: func() interface{} {
				return &scan.RowIterator{}
			},
		},
		batchPool: sync.Pool{
			New: func() interface{} {
				return &batchWrapper{}
			},
		},
		txnPool: sync.Pool{
			New: func() interface{} {
				return &transactionWrapper{}
			},
		},
		recordPool: sync.Pool{
			New: func() interface{} {
				return &types.Record{
					Data: make(map[string]*types.Value),
				}
			},
		},
		valuePool: sync.Pool{
			New: func() interface{} {
				return &types.Value{}
			},
		},
		bufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256) // Start with reasonable buffer size
			},
		},
		keyEncoderPool: sync.Pool{
			New: func() interface{} {
				return codec.NewMemComparableCodec()
			},
		},
		filterExprPool: sync.Pool{
			New: func() interface{} {
				return &engineTypes.FilterExpression{}
			},
		},
		// Enhanced pools
		scanResultPool: sync.Pool{
			New: func() interface{} {
				return make([]*types.Record, 0, 10) // Pre-allocate slice with capacity
			},
		},
		indexKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64) // Index keys are typically smaller
			},
		},
		tableKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 32) // Table keys are typically small
			},
		},
		metaKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128) // Meta keys can be larger
			},
		},
		compositeKeyPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 128) // Composite keys vary in size
			},
		},
		// Size-tiered buffer pools
		smallBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 64)
			},
		},
		mediumBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 256)
			},
		},
		largeBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 1024)
			},
		},
		hugeBufferPool: sync.Pool{
			New: func() interface{} {
				return make([]byte, 0, 4096)
			},
		},
		metrics: NewResourceMetricsCollector(),
	}
}

// batchWrapper wraps a storage batch for pooling
type batchWrapper struct {
	batch storage.Batch
}

// transactionWrapper wraps a transaction for pooling
type transactionWrapper struct {
	txn engineTypes.Transaction
}

// AcquireIterator acquires an iterator from the pool
func (rm *ResourceManager) AcquireIterator() *scan.RowIterator {
	start := time.Now()
	iter := rm.iteratorPool.Get()
	fromPool := iter != nil
	
	if !fromPool {
		iter = &scan.RowIterator{}
	}
	
	rm.metrics.RecordIteratorAcquire(start, fromPool)
	return iter.(*scan.RowIterator)
}

// ReleaseIterator releases an iterator back to the pool
func (rm *ResourceManager) ReleaseIterator(iter *scan.RowIterator) {
	start := time.Now()
	
	// Reset the iterator state
	iter.Reset()
	
	rm.iteratorPool.Put(iter)
	rm.metrics.RecordIteratorRelease(start)
}

// AcquireBatch acquires a batch from the pool
func (rm *ResourceManager) AcquireBatch() *batchWrapper {
	start := time.Now()
	batch := rm.batchPool.Get()
	fromPool := batch != nil
	
	if !fromPool {
		batch = &batchWrapper{}
	}
	
	rm.metrics.RecordBatchAcquire(start, fromPool)
	return batch.(*batchWrapper)
}

// ReleaseBatch releases a batch back to the pool
func (rm *ResourceManager) ReleaseBatch(batch *batchWrapper) {
	start := time.Now()
	
	batch.batch = nil
	rm.batchPool.Put(batch)
	
	rm.metrics.RecordBatchRelease(start)
}

// AcquireTransaction acquires a transaction from the pool
func (rm *ResourceManager) AcquireTransaction() *transactionWrapper {
	start := time.Now()
	txn := rm.txnPool.Get()
	fromPool := txn != nil
	
	if !fromPool {
		txn = &transactionWrapper{}
	}
	
	rm.metrics.RecordTransactionAcquire(start, fromPool)
	return txn.(*transactionWrapper)
}

// ReleaseTransaction releases a transaction back to the pool
func (rm *ResourceManager) ReleaseTransaction(txn *transactionWrapper) {
	start := time.Now()
	
	txn.txn = nil
	rm.txnPool.Put(txn)
	
	rm.metrics.RecordTransactionRelease(start)
}

// AcquireRecord acquires a record from the pool
func (rm *ResourceManager) AcquireRecord() *types.Record {
	start := time.Now()
	record := rm.recordPool.Get()
	fromPool := record != nil
	
	if !fromPool {
		record = &types.Record{
			Data: make(map[string]*types.Value),
		}
		rm.metrics.RecordRecordAcquire(start, false)
		return record.(*types.Record)
	}
	
	rm.metrics.RecordRecordAcquire(start, true)
	return record.(*types.Record)
}

// ReleaseRecord releases a record back to the pool
func (rm *ResourceManager) ReleaseRecord(record *types.Record) {
	start := time.Now()
	
	// Clear the map without reallocating
	for k := range record.Data {
		delete(record.Data, k)
	}
	record.ID = ""
	record.Table = ""
	record.Metadata = nil
	record.CreatedAt = time.Time{}
	record.UpdatedAt = time.Time{}
	record.Version = 0
	
	rm.recordPool.Put(record)
	rm.metrics.RecordRecordRelease(start)
}

// AcquireBuffer acquires a byte buffer from the pool with the specified size
func (rm *ResourceManager) AcquireBuffer(size int) []byte {
	start := time.Now()
	buf := rm.bufferPool.Get()
	fromPool := buf != nil
	
	if !fromPool {
		// Create a new buffer with the requested size
		buf = make([]byte, 0, size)
		rm.metrics.RecordBufferAcquire(start, false)
		return buf.([]byte)[:size]
	}
	
	b := buf.([]byte)
	if cap(b) >= size {
		// Reuse existing buffer
		rm.metrics.RecordBufferAcquire(start, true)
		return b[:size]
	}
	
	// Buffer too small, create a new one
	rm.metrics.RecordBufferAcquire(start, false)
	return make([]byte, size)
}

// ReleaseBuffer releases a byte buffer back to the pool
func (rm *ResourceManager) ReleaseBuffer(buf []byte) {
	start := time.Now()
	
	// Reset buffer without reallocating
	buf = buf[:0]
	rm.bufferPool.Put(buf)
	
	rm.metrics.RecordBufferRelease(start)
}

// AcquireScanResult acquires a scan result slice from the pool
func (rm *ResourceManager) AcquireScanResult() []*types.Record {
	start := time.Now()
	result := rm.scanResultPool.Get()
	fromPool := result != nil
	
	if !fromPool {
		result = make([]*types.Record, 0, 10)
		rm.metrics.RecordScanResultAcquire(start, false)
		return result.([]*types.Record)
	}
	
	rm.metrics.RecordScanResultAcquire(start, true)
	return result.([]*types.Record)
}

// ReleaseScanResult releases a scan result slice back to the pool
func (rm *ResourceManager) ReleaseScanResult(result []*types.Record) {
	start := time.Now()
	
	// Clear the slice without reallocating
	for i := range result {
		result[i] = nil
	}
	result = result[:0]
	
	rm.scanResultPool.Put(result)
	rm.metrics.RecordScanResultRelease(start)
}

// AcquireIndexKeyBuffer acquires a buffer optimized for index keys
func (rm *ResourceManager) AcquireIndexKeyBuffer(size int) []byte {
	start := time.Now()
	buf := rm.indexKeyPool.Get()
	fromPool := buf != nil
	
	if !fromPool {
		buf = make([]byte, 0, size)
		rm.metrics.RecordIndexKeyAcquire(start, false)
		return buf.([]byte)[:size]
	}
	
	b := buf.([]byte)
	if cap(b) >= size {
		rm.metrics.RecordIndexKeyAcquire(start, true)
		return b[:size]
	}
	
	// Buffer too small, create a new one
	rm.metrics.RecordIndexKeyAcquire(start, false)
	return make([]byte, size)
}

// ReleaseIndexKeyBuffer releases an index key buffer back to the pool
func (rm *ResourceManager) ReleaseIndexKeyBuffer(buf []byte) {
	start := time.Now()
	
	buf = buf[:0]
	rm.indexKeyPool.Put(buf)
	
	rm.metrics.RecordIndexKeyRelease(start)
}

// AcquireTableKeyBuffer acquires a buffer optimized for table keys
func (rm *ResourceManager) AcquireTableKeyBuffer(size int) []byte {
	start := time.Now()
	buf := rm.tableKeyPool.Get()
	fromPool := buf != nil
	
	if !fromPool {
		buf = make([]byte, 0, size)
		rm.metrics.RecordTableKeyAcquire(start, false)
		return buf.([]byte)[:size]
	}
	
	b := buf.([]byte)
	if cap(b) >= size {
		rm.metrics.RecordTableKeyAcquire(start, true)
		return b[:size]
	}
	
	rm.metrics.RecordTableKeyAcquire(start, false)
	return make([]byte, size)
}

// ReleaseTableKeyBuffer releases a table key buffer back to the pool
func (rm *ResourceManager) ReleaseTableKeyBuffer(buf []byte) {
	start := time.Now()
	
	buf = buf[:0]
	rm.tableKeyPool.Put(buf)
	
	rm.metrics.RecordTableKeyRelease(start)
}

// AcquireTieredBuffer acquires a buffer from the appropriate size-tiered pool
func (rm *ResourceManager) AcquireTieredBuffer(size int) []byte {
	start := time.Now()
	
	// Select the appropriate pool based on size
	var pool *sync.Pool
	var fromPool bool
	var buf interface{}
	
	switch {
	case size <= 64:
		pool = &rm.smallBufferPool
		buf = pool.Get()
		fromPool = buf != nil
		if !fromPool {
			buf = make([]byte, 0, 64)
		}
	case size <= 256:
		pool = &rm.mediumBufferPool
		buf = pool.Get()
		fromPool = buf != nil
		if !fromPool {
			buf = make([]byte, 0, 256)
		}
	case size <= 1024:
		pool = &rm.largeBufferPool
		buf = pool.Get()
		fromPool = buf != nil
		if !fromPool {
			buf = make([]byte, 0, 1024)
		}
	case size <= 4096:
		pool = &rm.hugeBufferPool
		buf = pool.Get()
		fromPool = buf != nil
		if !fromPool {
			buf = make([]byte, 0, 4096)
		}
	default:
		// For very large buffers, use the general buffer pool
		pool = &rm.bufferPool
		buf = pool.Get()
		fromPool = buf != nil
		if !fromPool {
			buf = make([]byte, 0, size)
		}
	}
	
	if fromPool {
		b := buf.([]byte)
		if cap(b) >= size {
			rm.metrics.RecordTieredBufferAcquire(start, true)
			return b[:size]
		}
	}
	
	rm.metrics.RecordTieredBufferAcquire(start, false)
	return make([]byte, size)
}

// ReleaseTieredBuffer releases a buffer back to its appropriate size-tiered pool
func (rm *ResourceManager) ReleaseTieredBuffer(buf []byte) {
	start := time.Now()
	
	buf = buf[:0]
	size := cap(buf)
	
	// Return to the appropriate pool based on size
	switch {
	case size <= 64:
		rm.smallBufferPool.Put(buf)
	case size <= 256:
		rm.mediumBufferPool.Put(buf)
	case size <= 1024:
		rm.largeBufferPool.Put(buf)
	case size <= 4096:
		rm.hugeBufferPool.Put(buf)
	default:
		rm.bufferPool.Put(buf)
	}
	
	rm.metrics.RecordTieredBufferRelease(start)
}

// GetResourceMetrics returns the current resource metrics
func (rm *ResourceManager) GetResourceMetrics() ResourceMetrics {
	return rm.metrics.GetMetrics()
}

// ResetResourceMetrics resets all resource metrics
func (rm *ResourceManager) ResetResourceMetrics() {
	rm.metrics.ResetMetrics()
}

// Initialize resource manager instance
var defaultResourceManager = NewResourceManager()

// GetResourceManager returns the default resource manager
func GetResourceManager() *ResourceManager {
	return defaultResourceManager
}