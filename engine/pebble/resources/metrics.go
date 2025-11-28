package resources

import (
	"sync/atomic"
	"time"
)

// ResourceMetrics tracks resource usage statistics
type ResourceMetrics struct {
	// Iterator metrics
	IteratorAcquired uint64
	IteratorReleased uint64
	IteratorCreated  uint64
	IteratorErrors   uint64
	
	// Batch metrics
	BatchAcquired uint64
	BatchReleased uint64
	BatchCreated  uint64
	BatchErrors   uint64
	
	// Transaction metrics
	TxnAcquired uint64
	TxnReleased uint64
	TxnCreated  uint64
	TxnErrors   uint64
	
	// Record metrics
	RecordAcquired uint64
	RecordReleased uint64
	RecordCreated  uint64
	RecordErrors   uint64
	
	// Buffer metrics
	BufferAcquired uint64
	BufferReleased uint64
	BufferCreated  uint64
	BufferErrors   uint64
	
	// Enhanced buffer metrics
	IndexKeyAcquired uint64
	IndexKeyReleased uint64
	IndexKeyCreated  uint64
	
	TableKeyAcquired uint64
	TableKeyReleased uint64
	TableKeyCreated  uint64
	
	ScanResultAcquired uint64
	ScanResultReleased uint64
	ScanResultCreated  uint64
	
	TieredBufferAcquired uint64
	TieredBufferReleased uint64
	TieredBufferCreated  uint64
	
	// Pool metrics
	PoolHits   uint64
	PoolMisses uint64
	
	// Timing metrics
	TotalAcquireTime int64
	TotalReleaseTime int64
}

// ResourceMetricsCollector collects and manages resource metrics
type ResourceMetricsCollector struct {
	metrics ResourceMetrics
}

// NewResourceMetricsCollector creates a new resource metrics collector
func NewResourceMetricsCollector() *ResourceMetricsCollector {
	return &ResourceMetricsCollector{}
}

// RecordIteratorAcquire records an iterator acquisition
func (rmc *ResourceMetricsCollector) RecordIteratorAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.IteratorAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.IteratorCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordIteratorRelease records an iterator release
func (rmc *ResourceMetricsCollector) RecordIteratorRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.IteratorReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordIteratorError records an iterator error
func (rmc *ResourceMetricsCollector) RecordIteratorError() {
	atomic.AddUint64(&rmc.metrics.IteratorErrors, 1)
}

// RecordBatchAcquire records a batch acquisition
func (rmc *ResourceMetricsCollector) RecordBatchAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.BatchAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.BatchCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordBatchRelease records a batch release
func (rmc *ResourceMetricsCollector) RecordBatchRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.BatchReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordBatchError records a batch error
func (rmc *ResourceMetricsCollector) RecordBatchError() {
	atomic.AddUint64(&rmc.metrics.BatchErrors, 1)
}

// RecordTransactionAcquire records a transaction acquisition
func (rmc *ResourceMetricsCollector) RecordTransactionAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.TxnAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.TxnCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTransactionRelease records a transaction release
func (rmc *ResourceMetricsCollector) RecordTransactionRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.TxnReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTransactionError records a transaction error
func (rmc *ResourceMetricsCollector) RecordTransactionError() {
	atomic.AddUint64(&rmc.metrics.TxnErrors, 1)
}

// RecordBufferAcquire records a buffer acquisition
func (rmc *ResourceMetricsCollector) RecordBufferAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.BufferAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.BufferCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordBufferRelease records a buffer release
func (rmc *ResourceMetricsCollector) RecordBufferRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.BufferReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordBufferError records a buffer error
func (rmc *ResourceMetricsCollector) RecordBufferError() {
	atomic.AddUint64(&rmc.metrics.BufferErrors, 1)
}

// RecordRecordAcquire records a record acquisition
func (rmc *ResourceMetricsCollector) RecordRecordAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.RecordAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.RecordCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordRecordRelease records a record release
func (rmc *ResourceMetricsCollector) RecordRecordRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.RecordReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordRecordError records a record error
func (rmc *ResourceMetricsCollector) RecordRecordError() {
	atomic.AddUint64(&rmc.metrics.RecordErrors, 1)
}

// RecordIndexKeyAcquire records an index key buffer acquisition
func (rmc *ResourceMetricsCollector) RecordIndexKeyAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.IndexKeyAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.IndexKeyCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordIndexKeyRelease records an index key buffer release
func (rmc *ResourceMetricsCollector) RecordIndexKeyRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.IndexKeyReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTableKeyAcquire records a table key buffer acquisition
func (rmc *ResourceMetricsCollector) RecordTableKeyAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.TableKeyAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.TableKeyCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTableKeyRelease records a table key buffer release
func (rmc *ResourceMetricsCollector) RecordTableKeyRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.TableKeyReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordScanResultAcquire records a scan result acquisition
func (rmc *ResourceMetricsCollector) RecordScanResultAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.ScanResultAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.ScanResultCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordScanResultRelease records a scan result release
func (rmc *ResourceMetricsCollector) RecordScanResultRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.ScanResultReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTieredBufferAcquire records a tiered buffer acquisition
func (rmc *ResourceMetricsCollector) RecordTieredBufferAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&rmc.metrics.TieredBufferAcquired, 1)
	if fromPool {
		atomic.AddUint64(&rmc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&rmc.metrics.PoolMisses, 1)
		atomic.AddUint64(&rmc.metrics.TieredBufferCreated, 1)
	}
	atomic.AddInt64(&rmc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTieredBufferRelease records a tiered buffer release
func (rmc *ResourceMetricsCollector) RecordTieredBufferRelease(start time.Time) {
	atomic.AddUint64(&rmc.metrics.TieredBufferReleased, 1)
	atomic.AddInt64(&rmc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// GetMetrics returns a copy of the current metrics
func (rmc *ResourceMetricsCollector) GetMetrics() ResourceMetrics {
	return ResourceMetrics{
		IteratorAcquired: atomic.LoadUint64(&rmc.metrics.IteratorAcquired),
		IteratorReleased: atomic.LoadUint64(&rmc.metrics.IteratorReleased),
		IteratorCreated:  atomic.LoadUint64(&rmc.metrics.IteratorCreated),
		IteratorErrors:   atomic.LoadUint64(&rmc.metrics.IteratorErrors),
		BatchAcquired:    atomic.LoadUint64(&rmc.metrics.BatchAcquired),
		BatchReleased:    atomic.LoadUint64(&rmc.metrics.BatchReleased),
		BatchCreated:     atomic.LoadUint64(&rmc.metrics.BatchCreated),
		BatchErrors:      atomic.LoadUint64(&rmc.metrics.BatchErrors),
		TxnAcquired:      atomic.LoadUint64(&rmc.metrics.TxnAcquired),
		TxnReleased:      atomic.LoadUint64(&rmc.metrics.TxnReleased),
		TxnCreated:       atomic.LoadUint64(&rmc.metrics.TxnCreated),
		TxnErrors:        atomic.LoadUint64(&rmc.metrics.TxnErrors),
		RecordAcquired:   atomic.LoadUint64(&rmc.metrics.RecordAcquired),
		RecordReleased:   atomic.LoadUint64(&rmc.metrics.RecordReleased),
		RecordCreated:    atomic.LoadUint64(&rmc.metrics.RecordCreated),
		RecordErrors:     atomic.LoadUint64(&rmc.metrics.RecordErrors),
		BufferAcquired:   atomic.LoadUint64(&rmc.metrics.BufferAcquired),
		BufferReleased:   atomic.LoadUint64(&rmc.metrics.BufferReleased),
		BufferCreated:    atomic.LoadUint64(&rmc.metrics.BufferCreated),
		BufferErrors:     atomic.LoadUint64(&rmc.metrics.BufferErrors),
		IndexKeyAcquired: atomic.LoadUint64(&rmc.metrics.IndexKeyAcquired),
		IndexKeyReleased: atomic.LoadUint64(&rmc.metrics.IndexKeyReleased),
		IndexKeyCreated:  atomic.LoadUint64(&rmc.metrics.IndexKeyCreated),
		TableKeyAcquired: atomic.LoadUint64(&rmc.metrics.TableKeyAcquired),
		TableKeyReleased: atomic.LoadUint64(&rmc.metrics.TableKeyReleased),
		TableKeyCreated:  atomic.LoadUint64(&rmc.metrics.TableKeyCreated),
		ScanResultAcquired: atomic.LoadUint64(&rmc.metrics.ScanResultAcquired),
		ScanResultReleased: atomic.LoadUint64(&rmc.metrics.ScanResultReleased),
		ScanResultCreated:  atomic.LoadUint64(&rmc.metrics.ScanResultCreated),
		TieredBufferAcquired: atomic.LoadUint64(&rmc.metrics.TieredBufferAcquired),
		TieredBufferReleased: atomic.LoadUint64(&rmc.metrics.TieredBufferReleased),
		TieredBufferCreated:  atomic.LoadUint64(&rmc.metrics.TieredBufferCreated),
		PoolHits:         atomic.LoadUint64(&rmc.metrics.PoolHits),
		PoolMisses:       atomic.LoadUint64(&rmc.metrics.PoolMisses),
		TotalAcquireTime: atomic.LoadInt64(&rmc.metrics.TotalAcquireTime),
		TotalReleaseTime: atomic.LoadInt64(&rmc.metrics.TotalReleaseTime),
	}
}

// ResetMetrics resets all metrics to zero
func (rmc *ResourceMetricsCollector) ResetMetrics() {
	atomic.StoreUint64(&rmc.metrics.IteratorAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.IteratorReleased, 0)
	atomic.StoreUint64(&rmc.metrics.IteratorCreated, 0)
	atomic.StoreUint64(&rmc.metrics.IteratorErrors, 0)
	atomic.StoreUint64(&rmc.metrics.BatchAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.BatchReleased, 0)
	atomic.StoreUint64(&rmc.metrics.BatchCreated, 0)
	atomic.StoreUint64(&rmc.metrics.BatchErrors, 0)
	atomic.StoreUint64(&rmc.metrics.TxnAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.TxnReleased, 0)
	atomic.StoreUint64(&rmc.metrics.TxnCreated, 0)
	atomic.StoreUint64(&rmc.metrics.TxnErrors, 0)
	atomic.StoreUint64(&rmc.metrics.RecordAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.RecordReleased, 0)
	atomic.StoreUint64(&rmc.metrics.RecordCreated, 0)
	atomic.StoreUint64(&rmc.metrics.RecordErrors, 0)
	atomic.StoreUint64(&rmc.metrics.BufferAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.BufferReleased, 0)
	atomic.StoreUint64(&rmc.metrics.BufferCreated, 0)
	atomic.StoreUint64(&rmc.metrics.BufferErrors, 0)
	atomic.StoreUint64(&rmc.metrics.IndexKeyAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.IndexKeyReleased, 0)
	atomic.StoreUint64(&rmc.metrics.IndexKeyCreated, 0)
	atomic.StoreUint64(&rmc.metrics.TableKeyAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.TableKeyReleased, 0)
	atomic.StoreUint64(&rmc.metrics.TableKeyCreated, 0)
	atomic.StoreUint64(&rmc.metrics.ScanResultAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.ScanResultReleased, 0)
	atomic.StoreUint64(&rmc.metrics.ScanResultCreated, 0)
	atomic.StoreUint64(&rmc.metrics.TieredBufferAcquired, 0)
	atomic.StoreUint64(&rmc.metrics.TieredBufferReleased, 0)
	atomic.StoreUint64(&rmc.metrics.TieredBufferCreated, 0)
	atomic.StoreUint64(&rmc.metrics.PoolHits, 0)
	atomic.StoreUint64(&rmc.metrics.PoolMisses, 0)
	atomic.StoreInt64(&rmc.metrics.TotalAcquireTime, 0)
	atomic.StoreInt64(&rmc.metrics.TotalReleaseTime, 0)
}