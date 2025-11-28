package resources

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/engine/pebble/leak_detection"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/types"
)

// AcquireIterator acquires an iterator from the pool
func (rm *ResourceManager) AcquireIterator() *scan.RowIterator {
	start := time.Now()
	iter := rm.iteratorPool.Get()
	fromPool := iter != nil
	
	if !fromPool {
		iter = &scan.RowIterator{}
	}
	
	// Track iterator for leak detection
	rowIter := iter.(*scan.RowIterator)
	if rm.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		tracked := rm.leakDetector.TrackIterator(rowIter, stackTrace)
		// Store the tracked resource in the iterator for later release
		rowIter.SetTrackedResource(tracked)
	}
	
	rm.metrics.RecordIteratorAcquire(start, fromPool)
	return rowIter
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