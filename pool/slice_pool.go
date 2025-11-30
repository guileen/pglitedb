// Package pool provides object pooling functionality for PGLiteDB
package pool

import (
	"sync"
	"sync/atomic"
)

// SlicePool is a specialized pool for frequently used slice types
type SlicePool struct {
	stringSlicePool    *sync.Pool
	columnInfoSlicePool *sync.Pool
	interfaceSlicePool  *sync.Pool
	metrics            atomicMetrics
}

// NewSlicePool creates a new SlicePool
func NewSlicePool() *SlicePool {
	return &SlicePool{
		stringSlicePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate slices with reasonable capacity
				slice := make([]string, 0, 16)
				return &slice
			},
		},
		columnInfoSlicePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate slices with reasonable capacity
				slice := make([]ColumnInfo, 0, 8)
				return &slice
			},
		},
		interfaceSlicePool: &sync.Pool{
			New: func() interface{} {
				// Pre-allocate slices with reasonable capacity
				slice := make([]interface{}, 0, 16)
				return &slice
			},
		},
	}
}

// GetStrings retrieves a string slice from the pool
func (sp *SlicePool) GetStrings(capacity int) *[]string {
	atomic.AddInt64(&sp.metrics.Gets, 1)
	slicePtr := sp.stringSlicePool.Get().(*[]string)
	if slicePtr == nil {
		atomic.AddInt64(&sp.metrics.Misses, 1)
		slice := make([]string, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&sp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]string, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutStrings returns a string slice to the pool
func (sp *SlicePool) PutStrings(slice *[]string) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&sp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 128 {
		*slice = (*slice)[:0] // Reset length
		sp.stringSlicePool.Put(slice)
	}
}

// GetColumnInfos retrieves a ColumnInfo slice from the pool
func (sp *SlicePool) GetColumnInfos(capacity int) *[]ColumnInfo {
	atomic.AddInt64(&sp.metrics.Gets, 1)
	slicePtr := sp.columnInfoSlicePool.Get().(*[]ColumnInfo)
	if slicePtr == nil {
		atomic.AddInt64(&sp.metrics.Misses, 1)
		slice := make([]ColumnInfo, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&sp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]ColumnInfo, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutColumnInfos returns a ColumnInfo slice to the pool
func (sp *SlicePool) PutColumnInfos(slice *[]ColumnInfo) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&sp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 64 {
		*slice = (*slice)[:0] // Reset length
		sp.columnInfoSlicePool.Put(slice)
	}
}

// GetInterfaces retrieves an interface{} slice from the pool
func (sp *SlicePool) GetInterfaces(capacity int) *[]interface{} {
	atomic.AddInt64(&sp.metrics.Gets, 1)
	slicePtr := sp.interfaceSlicePool.Get().(*[]interface{})
	if slicePtr == nil {
		atomic.AddInt64(&sp.metrics.Misses, 1)
		slice := make([]interface{}, 0, capacity)
		return &slice
	}
	atomic.AddInt64(&sp.metrics.Hits, 1)
	slice := *slicePtr
	// Ensure capacity is sufficient
	if cap(slice) < capacity {
		slice = make([]interface{}, 0, capacity)
	} else {
		slice = slice[:0] // Reset length but keep capacity
	}
	return &slice
}

// PutInterfaces returns an interface{} slice to the pool
func (sp *SlicePool) PutInterfaces(slice *[]interface{}) {
	if slice == nil {
		return
	}
	atomic.AddInt64(&sp.metrics.Puts, 1)
	// Only put back slices that aren't too large to avoid memory bloat
	if cap(*slice) <= 128 {
		*slice = (*slice)[:0] // Reset length
		sp.interfaceSlicePool.Put(slice)
	}
}

// Metrics returns the current pool metrics
func (sp *SlicePool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&sp.metrics.Gets),
		Puts:   atomic.LoadInt64(&sp.metrics.Puts),
		Hits:   atomic.LoadInt64(&sp.metrics.Hits),
		Misses: atomic.LoadInt64(&sp.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}

// ColumnInfo is a copy of the types.ColumnInfo to avoid circular dependencies
type ColumnInfo struct {
	Name string
	Type ColumnType
}

// ColumnType is a copy of the types.ColumnType to avoid circular dependencies
type ColumnType int

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeInt
	ColumnTypeFloat
	ColumnTypeBool
	ColumnTypeTime
)