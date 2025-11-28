package pebble

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
	atomic.StoreUint64(&rmc.metrics.PoolHits, 0)
	atomic.StoreUint64(&rmc.metrics.PoolMisses, 0)
	atomic.StoreInt64(&rmc.metrics.TotalAcquireTime, 0)
	atomic.StoreInt64(&rmc.metrics.TotalReleaseTime, 0)
}