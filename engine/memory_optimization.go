// Advanced memory optimization utilities for the storage engine with GC tuning
package engine

import (
	"runtime"
	"sync"
	"sync/atomic"
	
	"github.com/guileen/pglitedb/types"
)

// BufferPool manages reusable byte buffers to reduce allocations with advanced GC tuning
type BufferPool struct {
	pool        sync.Pool
	stats       BufferPoolStats
	sizeClasses []int // Predefined size classes for better memory utilization
}

// BufferPoolStats contains statistics about buffer pool usage
type BufferPoolStats struct {
	Acquired      uint64
	Released      uint64
	Allocated     uint64
	HitRate       float64
	AvgBufferSize uint64
}

// ResultSetPool manages reusable ResultSet objects to reduce allocations
type ResultSetPool struct {
	pool  sync.Pool
	stats ResultSetPoolStats
}

// ResultSetPoolStats contains statistics about ResultSet pool usage
type ResultSetPoolStats struct {
	Acquired uint64
	Released uint64
	Allocated uint64
	HitRate  float64
}

// NewBufferPool creates a new buffer pool with predefined size classes
func NewBufferPool() *BufferPool {
	return &BufferPool{
		sizeClasses: []int{128, 256, 512, 1024, 2048, 4096, 8192, 16384, 32768, 65536}, // Powers of 2 with some intermediate values
		pool: sync.Pool{
			New: func() interface{} {
				// Start with 1KB buffers
				return make([]byte, 0, 1024)
			},
		},
	}
}

// AcquireBuffer gets a buffer from the pool, optimizing for size classes
func (bp *BufferPool) AcquireBuffer(size int) []byte {
	atomic.AddUint64(&bp.stats.Acquired, 1)
	
	// Round up to nearest size class for better reuse
	roundedSize := bp.roundUpToSizeClass(size)
	
	buf := bp.pool.Get().([]byte)
	if cap(buf) < roundedSize {
		// Need a larger buffer, allocate new one
		atomic.AddUint64(&bp.stats.Allocated, 1)
		buf = make([]byte, roundedSize)
	} else {
		// Resize existing buffer
		buf = buf[:roundedSize]
	}
	
	// Update statistics
	currentAvg := atomic.LoadUint64(&bp.stats.AvgBufferSize)
	newAvg := (currentAvg + uint64(roundedSize)) / 2
	atomic.StoreUint64(&bp.stats.AvgBufferSize, newAvg)
	
	// Update hit rate
	acquired := atomic.LoadUint64(&bp.stats.Acquired)
	allocated := atomic.LoadUint64(&bp.stats.Allocated)
	if acquired > 0 {
		hitRate := float64(acquired-allocated) / float64(acquired)
		atomic.StoreUint64((*uint64)(&bp.stats.HitRate), uint64(hitRate*1000000)) // Store as integer for atomic operations
	}
	
	return buf
}

// ReleaseBuffer returns a buffer to the pool with GC-aware management
func (bp *BufferPool) ReleaseBuffer(buf []byte) {
	atomic.AddUint64(&bp.stats.Released, 1)
	
	// Reset length but keep capacity
	buf = buf[:0]
	
	// Periodically trigger GC if we have too many buffers (GC tuning)
	released := atomic.LoadUint64(&bp.stats.Released)
	acquired := atomic.LoadUint64(&bp.stats.Acquired)
	if released%1000 == 0 && acquired > 0 {
		// If hit rate is low, suggest GC
		hitRate := float64(atomic.LoadUint64((*uint64)(&bp.stats.HitRate))) / 1000000.0
		if hitRate < 0.7 { // Less than 70% hit rate
			runtime.GC() // Proactive GC tuning
		}
	}
	
	bp.pool.Put(buf)
}

// roundUpToSizeClass rounds a size up to the nearest predefined size class
func (bp *BufferPool) roundUpToSizeClass(size int) int {
	for _, class := range bp.sizeClasses {
		if size <= class {
			return class
		}
	}
	// If larger than largest class, round up to next power of 2
	power := 65536
	for power < size {
		power *= 2
	}
	return power
}

// Stats returns current buffer pool statistics
func (bp *BufferPool) Stats() BufferPoolStats {
	return BufferPoolStats{
		Acquired:      atomic.LoadUint64(&bp.stats.Acquired),
		Released:      atomic.LoadUint64(&bp.stats.Released),
		Allocated:     atomic.LoadUint64(&bp.stats.Allocated),
		HitRate:       float64(atomic.LoadUint64((*uint64)(&bp.stats.HitRate))) / 1000000.0,
		AvgBufferSize: atomic.LoadUint64(&bp.stats.AvgBufferSize),
	}
}

// NewResultSetPool creates a new ResultSet pool
func NewResultSetPool() *ResultSetPool {
	return &ResultSetPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &types.ResultSet{
					Columns: make([]string, 0, 8),
					Rows:    make([][]interface{}, 0, 16),
				}
			},
		},
	}
}

// AcquireResultSet gets a ResultSet from the pool
func (rsp *ResultSetPool) AcquireResultSet() *types.ResultSet {
	atomic.AddUint64(&rsp.stats.Acquired, 1)
	
	rs := rsp.pool.Get().(*types.ResultSet)
	if rs.Columns == nil {
		atomic.AddUint64(&rsp.stats.Allocated, 1)
		rs.Columns = make([]string, 0, 8)
		rs.Rows = make([][]interface{}, 0, 16)
	} else {
		// Reset slices but keep capacity
		rs.Columns = rs.Columns[:0]
		rs.Rows = rs.Rows[:0]
		rs.Count = 0
		rs.LastInsertID = 0
	}
	
	// Update hit rate
	acquired := atomic.LoadUint64(&rsp.stats.Acquired)
	allocated := atomic.LoadUint64(&rsp.stats.Allocated)
	if acquired > 0 {
		hitRate := float64(acquired-allocated) / float64(acquired)
		rsp.stats.HitRate = hitRate
	}
	
	return rs
}

// ReleaseResultSet returns a ResultSet to the pool
func (rsp *ResultSetPool) ReleaseResultSet(rs *types.ResultSet) {
	atomic.AddUint64(&rsp.stats.Released, 1)
	
	// Clear sensitive data
	for i := range rs.Rows {
		for j := range rs.Rows[i] {
			rs.Rows[i][j] = nil
		}
		rs.Rows[i] = nil
	}
	rs.Rows = rs.Rows[:0]
	
	rsp.pool.Put(rs)
}

// ResultSetStats returns current ResultSet pool statistics
func (rsp *ResultSetPool) Stats() ResultSetPoolStats {
	return ResultSetPoolStats{
		Acquired:  atomic.LoadUint64(&rsp.stats.Acquired),
		Released:  atomic.LoadUint64(&rsp.stats.Released),
		Allocated: atomic.LoadUint64(&rsp.stats.Allocated),
		HitRate:   rsp.stats.HitRate,
	}
}

// Global pools for the engine
var (
	globalBufferPool   = NewBufferPool()
	globalResultSetPool = NewResultSetPool()
)

// AcquireBuffer is a convenience function to get a buffer
func AcquireBuffer(size int) []byte {
	return globalBufferPool.AcquireBuffer(size)
}

// ReleaseBuffer is a convenience function to return a buffer
func ReleaseBuffer(buf []byte) {
	globalBufferPool.ReleaseBuffer(buf)
}

// AcquireResultSet is a convenience function to get a ResultSet
func AcquireResultSet() *types.ResultSet {
	return globalResultSetPool.AcquireResultSet()
}

// ReleaseResultSet is a convenience function to return a ResultSet
func ReleaseResultSet(rs *types.ResultSet) {
	globalResultSetPool.ReleaseResultSet(rs)
}

// MemoryManager provides centralized memory management with GC tuning
type MemoryManager struct {
	bufferPool   *BufferPool
	resultSetPool *ResultSetPool
}

// NewMemoryManager creates a new memory manager
func NewMemoryManager() *MemoryManager {
	return &MemoryManager{
		bufferPool:   globalBufferPool,
		resultSetPool: globalResultSetPool,
	}
}

// GetBufferPoolStats returns buffer pool statistics
func (mm *MemoryManager) GetBufferPoolStats() BufferPoolStats {
	return mm.bufferPool.Stats()
}

// GetResultSetPoolStats returns ResultSet pool statistics
func (mm *MemoryManager) GetResultSetPoolStats() ResultSetPoolStats {
	return mm.resultSetPool.Stats()
}

// ForceGC triggers garbage collection and returns memory statistics
func (mm *MemoryManager) ForceGC() runtime.MemStats {
	var m runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&m)
	return m
}

// TuneGC adjusts GC target percentage based on allocation patterns
func (mm *MemoryManager) TuneGC() {
	// Get current memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	
	// Adjust GC target based on allocation rate
	// If we're allocating a lot, increase GC target to reduce frequency
	// If we have high hit rates in pools, decrease GC target to collect more aggressively
	bufferHitRate := mm.bufferPool.Stats().HitRate
	resultSetHitRate := mm.resultSetPool.Stats().HitRate
	
	currentTarget := runtime.GCPercent()
	newTarget := currentTarget
	
	if bufferHitRate > 0.8 && resultSetHitRate > 0.8 {
		// High hit rates mean we're reusing objects well, can afford more aggressive GC
		newTarget = currentTarget - 5
		if newTarget < 20 {
			newTarget = 20 // Minimum 20%
		}
	} else if bufferHitRate < 0.5 || resultSetHitRate < 0.5 {
		// Low hit rates mean we're allocating more, need to GC less frequently
		newTarget = currentTarget + 10
		if newTarget > 100 {
			newTarget = 100 // Maximum 100%
		}
	}
	
	if newTarget != currentTarget {
		runtime.SetGCPercent(newTarget)
	}
}