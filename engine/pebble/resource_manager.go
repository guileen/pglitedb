package pebble

import (
	"sync"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
)

// ResourceManager manages resource pooling for the pebble engine
type ResourceManager struct {
	iteratorPool sync.Pool
	batchPool    sync.Pool
	txnPool      sync.Pool
	metrics      *ResourceMetricsCollector
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