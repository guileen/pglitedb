package metrics

import (
	"sync/atomic"
	"time"
)

// Metrics tracks resource usage statistics
type Metrics struct {
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

// Collector collects and manages resource metrics
type Collector struct {
	metrics Metrics
}

// NewCollector creates a new resource metrics collector
func NewCollector() *Collector {
	return &Collector{}
}

// RecordIteratorAcquire records an iterator acquisition
func (mc *Collector) RecordIteratorAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.IteratorAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.IteratorCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordIteratorRelease records an iterator release
func (mc *Collector) RecordIteratorRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.IteratorReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordIteratorError records an iterator error
func (mc *Collector) RecordIteratorError() {
	atomic.AddUint64(&mc.metrics.IteratorErrors, 1)
}

// RecordBatchAcquire records a batch acquisition
func (mc *Collector) RecordBatchAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.BatchAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.BatchCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordBatchRelease records a batch release
func (mc *Collector) RecordBatchRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.BatchReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordBatchError records a batch error
func (mc *Collector) RecordBatchError() {
	atomic.AddUint64(&mc.metrics.BatchErrors, 1)
}

// RecordTransactionAcquire records a transaction acquisition
func (mc *Collector) RecordTransactionAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.TxnAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.TxnCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTransactionRelease records a transaction release
func (mc *Collector) RecordTransactionRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.TxnReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTransactionError records a transaction error
func (mc *Collector) RecordTransactionError() {
	atomic.AddUint64(&mc.metrics.TxnErrors, 1)
}

// RecordBufferAcquire records a buffer acquisition
func (mc *Collector) RecordBufferAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.BufferAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.BufferCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordBufferRelease records a buffer release
func (mc *Collector) RecordBufferRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.BufferReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordBufferError records a buffer error
func (mc *Collector) RecordBufferError() {
	atomic.AddUint64(&mc.metrics.BufferErrors, 1)
}

// RecordRecordAcquire records a record acquisition
func (mc *Collector) RecordRecordAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.RecordAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.RecordCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordRecordRelease records a record release
func (mc *Collector) RecordRecordRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.RecordReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordRecordError records a record error
func (mc *Collector) RecordRecordError() {
	atomic.AddUint64(&mc.metrics.RecordErrors, 1)
}

// RecordIndexKeyAcquire records an index key buffer acquisition
func (mc *Collector) RecordIndexKeyAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.IndexKeyAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.IndexKeyCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordIndexKeyRelease records an index key buffer release
func (mc *Collector) RecordIndexKeyRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.IndexKeyReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTableKeyAcquire records a table key buffer acquisition
func (mc *Collector) RecordTableKeyAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.TableKeyAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.TableKeyCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTableKeyRelease records a table key buffer release
func (mc *Collector) RecordTableKeyRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.TableKeyReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordScanResultAcquire records a scan result acquisition
func (mc *Collector) RecordScanResultAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.ScanResultAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.ScanResultCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordScanResultRelease records a scan result release
func (mc *Collector) RecordScanResultRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.ScanResultReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// RecordTieredBufferAcquire records a tiered buffer acquisition
func (mc *Collector) RecordTieredBufferAcquire(start time.Time, fromPool bool) {
	atomic.AddUint64(&mc.metrics.TieredBufferAcquired, 1)
	if fromPool {
		atomic.AddUint64(&mc.metrics.PoolHits, 1)
	} else {
		atomic.AddUint64(&mc.metrics.PoolMisses, 1)
		atomic.AddUint64(&mc.metrics.TieredBufferCreated, 1)
	}
	atomic.AddInt64(&mc.metrics.TotalAcquireTime, time.Since(start).Nanoseconds())
}

// RecordTieredBufferRelease records a tiered buffer release
func (mc *Collector) RecordTieredBufferRelease(start time.Time) {
	atomic.AddUint64(&mc.metrics.TieredBufferReleased, 1)
	atomic.AddInt64(&mc.metrics.TotalReleaseTime, time.Since(start).Nanoseconds())
}

// GetMetrics returns a copy of the current metrics
func (mc *Collector) GetMetrics() Metrics {
	return Metrics{
		IteratorAcquired: atomic.LoadUint64(&mc.metrics.IteratorAcquired),
		IteratorReleased: atomic.LoadUint64(&mc.metrics.IteratorReleased),
		IteratorCreated:  atomic.LoadUint64(&mc.metrics.IteratorCreated),
		IteratorErrors:   atomic.LoadUint64(&mc.metrics.IteratorErrors),
		BatchAcquired:    atomic.LoadUint64(&mc.metrics.BatchAcquired),
		BatchReleased:    atomic.LoadUint64(&mc.metrics.BatchReleased),
		BatchCreated:     atomic.LoadUint64(&mc.metrics.BatchCreated),
		BatchErrors:      atomic.LoadUint64(&mc.metrics.BatchErrors),
		TxnAcquired:      atomic.LoadUint64(&mc.metrics.TxnAcquired),
		TxnReleased:      atomic.LoadUint64(&mc.metrics.TxnReleased),
		TxnCreated:       atomic.LoadUint64(&mc.metrics.TxnCreated),
		TxnErrors:        atomic.LoadUint64(&mc.metrics.TxnErrors),
		RecordAcquired:   atomic.LoadUint64(&mc.metrics.RecordAcquired),
		RecordReleased:   atomic.LoadUint64(&mc.metrics.RecordReleased),
		RecordCreated:    atomic.LoadUint64(&mc.metrics.RecordCreated),
		RecordErrors:     atomic.LoadUint64(&mc.metrics.RecordErrors),
		BufferAcquired:   atomic.LoadUint64(&mc.metrics.BufferAcquired),
		BufferReleased:   atomic.LoadUint64(&mc.metrics.BufferReleased),
		BufferCreated:    atomic.LoadUint64(&mc.metrics.BufferCreated),
		BufferErrors:     atomic.LoadUint64(&mc.metrics.BufferErrors),
		IndexKeyAcquired: atomic.LoadUint64(&mc.metrics.IndexKeyAcquired),
		IndexKeyReleased: atomic.LoadUint64(&mc.metrics.IndexKeyReleased),
		IndexKeyCreated:  atomic.LoadUint64(&mc.metrics.IndexKeyCreated),
		TableKeyAcquired: atomic.LoadUint64(&mc.metrics.TableKeyAcquired),
		TableKeyReleased: atomic.LoadUint64(&mc.metrics.TableKeyReleased),
		TableKeyCreated:  atomic.LoadUint64(&mc.metrics.TableKeyCreated),
		ScanResultAcquired: atomic.LoadUint64(&mc.metrics.ScanResultAcquired),
		ScanResultReleased: atomic.LoadUint64(&mc.metrics.ScanResultReleased),
		ScanResultCreated:  atomic.LoadUint64(&mc.metrics.ScanResultCreated),
		TieredBufferAcquired: atomic.LoadUint64(&mc.metrics.TieredBufferAcquired),
		TieredBufferReleased: atomic.LoadUint64(&mc.metrics.TieredBufferReleased),
		TieredBufferCreated:  atomic.LoadUint64(&mc.metrics.TieredBufferCreated),
		PoolHits:         atomic.LoadUint64(&mc.metrics.PoolHits),
		PoolMisses:       atomic.LoadUint64(&mc.metrics.PoolMisses),
		TotalAcquireTime: atomic.LoadInt64(&mc.metrics.TotalAcquireTime),
		TotalReleaseTime: atomic.LoadInt64(&mc.metrics.TotalReleaseTime),
	}
}

// ResetMetrics resets all metrics to zero
func (mc *Collector) ResetMetrics() {
	atomic.StoreUint64(&mc.metrics.IteratorAcquired, 0)
	atomic.StoreUint64(&mc.metrics.IteratorReleased, 0)
	atomic.StoreUint64(&mc.metrics.IteratorCreated, 0)
	atomic.StoreUint64(&mc.metrics.IteratorErrors, 0)
	atomic.StoreUint64(&mc.metrics.BatchAcquired, 0)
	atomic.StoreUint64(&mc.metrics.BatchReleased, 0)
	atomic.StoreUint64(&mc.metrics.BatchCreated, 0)
	atomic.StoreUint64(&mc.metrics.BatchErrors, 0)
	atomic.StoreUint64(&mc.metrics.TxnAcquired, 0)
	atomic.StoreUint64(&mc.metrics.TxnReleased, 0)
	atomic.StoreUint64(&mc.metrics.TxnCreated, 0)
	atomic.StoreUint64(&mc.metrics.TxnErrors, 0)
	atomic.StoreUint64(&mc.metrics.RecordAcquired, 0)
	atomic.StoreUint64(&mc.metrics.RecordReleased, 0)
	atomic.StoreUint64(&mc.metrics.RecordCreated, 0)
	atomic.StoreUint64(&mc.metrics.RecordErrors, 0)
	atomic.StoreUint64(&mc.metrics.BufferAcquired, 0)
	atomic.StoreUint64(&mc.metrics.BufferReleased, 0)
	atomic.StoreUint64(&mc.metrics.BufferCreated, 0)
	atomic.StoreUint64(&mc.metrics.BufferErrors, 0)
	atomic.StoreUint64(&mc.metrics.IndexKeyAcquired, 0)
	atomic.StoreUint64(&mc.metrics.IndexKeyReleased, 0)
	atomic.StoreUint64(&mc.metrics.IndexKeyCreated, 0)
	atomic.StoreUint64(&mc.metrics.TableKeyAcquired, 0)
	atomic.StoreUint64(&mc.metrics.TableKeyReleased, 0)
	atomic.StoreUint64(&mc.metrics.TableKeyCreated, 0)
	atomic.StoreUint64(&mc.metrics.ScanResultAcquired, 0)
	atomic.StoreUint64(&mc.metrics.ScanResultReleased, 0)
	atomic.StoreUint64(&mc.metrics.ScanResultCreated, 0)
	atomic.StoreUint64(&mc.metrics.TieredBufferAcquired, 0)
	atomic.StoreUint64(&mc.metrics.TieredBufferReleased, 0)
	atomic.StoreUint64(&mc.metrics.TieredBufferCreated, 0)
	atomic.StoreUint64(&mc.metrics.PoolHits, 0)
	atomic.StoreUint64(&mc.metrics.PoolMisses, 0)
	atomic.StoreInt64(&mc.metrics.TotalAcquireTime, 0)
	atomic.StoreInt64(&mc.metrics.TotalReleaseTime, 0)
}