package resources

import (
	"context"
	"sync"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/leak_detection"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/guileen/pglitedb/engine/pebble/resources/pools"
)

// ResourceManager manages resource pooling for the pebble engine
type ResourceManager struct {
	poolManager *pools.Manager
	metrics     *ResourceMetricsCollector
	leakDetector engineTypes.LeakDetector
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	leakDetector := leak_detection.NewLeakDetector()
	metricsCollector := NewResourceMetricsCollector()
	poolManager := pools.NewManager(leakDetector, metricsCollector)
	
	rm := &ResourceManager{
		poolManager:   poolManager,
		metrics:       metricsCollector,
		leakDetector:  leakDetector,
	}
	
	// Start leak detection monitoring
	rm.leakDetector.StartMonitoring(context.Background())
	
	return rm
}



// GetResourceManager returns the default resource manager
func GetResourceManager() *ResourceManager {
	return defaultResourceManager
}

// AcquireIterator gets an iterator from the pool
func (rm *ResourceManager) AcquireIterator() *scan.RowIterator {
	return rm.poolManager.AcquireIterator()
}

// ReleaseIterator returns an iterator to the pool
func (rm *ResourceManager) ReleaseIterator(iter *scan.RowIterator) {
	rm.poolManager.ReleaseIterator(iter)
}

// AcquireTransaction gets a transaction from the pool
func (rm *ResourceManager) AcquireTransaction() *pools.TransactionWrapper {
	return rm.poolManager.AcquireTransaction()
}

// ReleaseTransaction returns a transaction to the pool
func (rm *ResourceManager) ReleaseTransaction(txn *pools.TransactionWrapper) {
	rm.poolManager.ReleaseTransaction(txn)
}

// AcquireRecord gets a record from the pool
func (rm *ResourceManager) AcquireRecord() *types.Record {
	return rm.poolManager.AcquireRecord()
}

// ReleaseRecord returns a record to the pool
func (rm *ResourceManager) ReleaseRecord(record *types.Record) {
	rm.poolManager.ReleaseRecord(record)
}

// AcquireBuffer gets a byte buffer from the pool with the specified size
func (rm *ResourceManager) AcquireBuffer(size int) []byte {
	return rm.poolManager.AcquireBuffer(size)
}

// ReleaseBuffer returns a byte buffer to the pool
func (rm *ResourceManager) ReleaseBuffer(buf []byte) {
	rm.poolManager.ReleaseBuffer(buf)
}

// AcquireTieredBuffer gets a buffer from the appropriate size-tiered pool
func (rm *ResourceManager) AcquireTieredBuffer(size int) []byte {
	return rm.poolManager.AcquireTieredBuffer(size)
}

// ReleaseTieredBuffer returns a buffer back to its appropriate size-tiered pool
func (rm *ResourceManager) ReleaseTieredBuffer(buf []byte) {
	rm.poolManager.ReleaseTieredBuffer(buf)
}

// Initialize resource manager instance
var defaultResourceManager = NewResourceManager()