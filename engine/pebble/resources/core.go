package resources

import (
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
	"github.com/guileen/pglitedb/engine/pebble/resources/metrics"
	"github.com/guileen/pglitedb/engine/pebble/resources/pools"
	"github.com/guileen/pglitedb/engine/pebble/resources/sizing"
	"github.com/guileen/pglitedb/types"
)

// ResourceManager manages resource pooling for the pebble engine
// It acts as a coordinator delegating responsibilities to specialized packages
type ResourceManager struct {
	poolManager  *pools.Manager
	metrics      *metrics.Collector
	leakDetector *leak.Detector
	sizingManager *sizing.Manager
}

// NewResourceManager creates a new resource manager
func NewResourceManager() *ResourceManager {
	leakDetector := leak.NewDetector()
	metricsCollector := metrics.NewCollector()
	sizingManager := sizing.NewManager()
	sizingManager.InitializeDefaultSizes()
	poolManager := pools.NewManager(leakDetector, metricsCollector)
	
	rm := &ResourceManager{
		poolManager:   poolManager,
		metrics:       metricsCollector,
		leakDetector:  leakDetector,
		sizingManager: sizingManager,
	}
	
	return rm
}

// GetResourceManager returns the default resource manager
func GetResourceManager() *ResourceManager {
	return defaultResourceManager
}

// PoolManager returns the pool manager for direct access to resource pools
func (rm *ResourceManager) PoolManager() *pools.Manager {
	return rm.poolManager
}

// Metrics returns the metrics collector for accessing resource usage statistics
func (rm *ResourceManager) Metrics() *metrics.Collector {
	return rm.metrics
}

// LeakDetector returns the leak detector for leak detection functionality
func (rm *ResourceManager) LeakDetector() *leak.Detector {
	return rm.leakDetector
}

// SizingManager returns the sizing manager for adaptive pool sizing
func (rm *ResourceManager) SizingManager() *sizing.Manager {
	return rm.sizingManager
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

// AcquireIndexKeyBuffer gets a buffer optimized for index keys
func (rm *ResourceManager) AcquireIndexKeyBuffer(size int) []byte {
	return rm.poolManager.AcquireIndexKeyBuffer(size)
}

// ReleaseIndexKeyBuffer returns an index key buffer to the pool
func (rm *ResourceManager) ReleaseIndexKeyBuffer(buf []byte) {
	rm.poolManager.ReleaseIndexKeyBuffer(buf)
}

// AcquireTableKeyBuffer gets a buffer optimized for table keys
func (rm *ResourceManager) AcquireTableKeyBuffer(size int) []byte {
	return rm.poolManager.AcquireTableKeyBuffer(size)
}

// ReleaseTableKeyBuffer returns a table key buffer to the pool
func (rm *ResourceManager) ReleaseTableKeyBuffer(buf []byte) {
	rm.poolManager.ReleaseTableKeyBuffer(buf)
}

// AcquireScanResult gets a scan result slice from the pool
func (rm *ResourceManager) AcquireScanResult() []*types.Record {
	return rm.poolManager.AcquireScanResult()
}

// ReleaseScanResult returns a scan result slice to the pool
func (rm *ResourceManager) ReleaseScanResult(result []*types.Record) {
	rm.poolManager.ReleaseScanResult(result)
}

// TrackTransaction tracks a transaction for leak detection
func (rm *ResourceManager) TrackTransaction(txn interface{}) {
	rm.leakDetector.TrackTransaction(txn)
}

// TrackConnection tracks a connection for leak detection
func (rm *ResourceManager) TrackConnection(conn interface{}) {
	rm.leakDetector.TrackConnection(conn)
}

// TrackFileDescriptor tracks a file descriptor for leak detection
func (rm *ResourceManager) TrackFileDescriptor(fd interface{}, path string) {
	rm.leakDetector.TrackFileDescriptor(fd, path)
}

// TrackGoroutine tracks a goroutine for leak detection
func (rm *ResourceManager) TrackGoroutine(goroutineID uint64) {
	rm.leakDetector.TrackGoroutine(goroutineID)
}

// TrackCurrentGoroutine tracks the current goroutine for leak detection
func (rm *ResourceManager) TrackCurrentGoroutine() {
	rm.leakDetector.TrackCurrentGoroutine()
}

// CheckForLeaks checks for resource leaks and returns a report
func (rm *ResourceManager) CheckForLeaks() *engineTypes.LeakReport {
	return rm.leakDetector.CheckForLeaks()
}

// GetLeakDetector returns the leak detector
func (rm *ResourceManager) GetLeakDetector() engineTypes.LeakDetector {
	return rm.leakDetector.GetLeakDetector()
}

// GetMetrics returns current resource metrics
func (rm *ResourceManager) GetMetrics() metrics.Metrics {
	return rm.metrics.GetMetrics()
}

// ResetMetrics resets all metrics to zero
func (rm *ResourceManager) ResetMetrics() {
	rm.metrics.ResetMetrics()
}

// AdjustPoolSizes adapts pool sizes based on usage patterns
func (rm *ResourceManager) AdjustPoolSizes() {
	rm.sizingManager.AdjustPoolSizes(rm.metrics)
}

// ProactiveCleanup performs cleanup of unused resources
func (rm *ResourceManager) ProactiveCleanup() {
	// Trigger garbage collection if memory usage is high
	// This is a placeholder - actual implementation would check metrics
	// and trigger cleanup based on thresholds
}

// Initialize resource manager instance
var defaultResourceManager = NewResourceManager()