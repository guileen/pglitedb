// Package pool provides object pooling functionality for PGLiteDB
package pool

import (
	"sync"
	"sync/atomic"
)

// Pool interface defines the basic operations for all pool types
type Pool interface {
	Get() interface{}
	Put(interface{})
	Metrics() PoolMetrics
}

// PoolMetrics tracks pool performance and health
type PoolMetrics struct {
	Gets      int64 // Number of Get() operations
	Puts      int64 // Number of Put() operations
	Hits      int64 // Number of successful reuse operations
	Misses    int64 // Number of allocations due to pool miss
	Size      int64 // Current pool size
}

// SyncPool is a wrapper around Go's sync.Pool for simple object reuse
type SyncPool struct {
	pool    *sync.Pool
	name    string
	metrics atomicMetrics
}

// NewSyncPool creates a new SyncPool
func NewSyncPool(name string, factory func() interface{}) *SyncPool {
	return &SyncPool{
		pool: &sync.Pool{
			New: factory,
		},
		name: name,
	}
}

// Get retrieves an object from the pool
func (sp *SyncPool) Get() interface{} {
	atomic.AddInt64(&sp.metrics.Gets, 1)
	obj := sp.pool.Get()
	if obj == nil {
		atomic.AddInt64(&sp.metrics.Misses, 1)
	} else {
		atomic.AddInt64(&sp.metrics.Hits, 1)
	}
	return obj
}

// Put returns an object to the pool
func (sp *SyncPool) Put(obj interface{}) {
	atomic.AddInt64(&sp.metrics.Puts, 1)
	sp.pool.Put(obj)
}

// Metrics returns the current pool metrics
func (sp *SyncPool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&sp.metrics.Gets),
		Puts:   atomic.LoadInt64(&sp.metrics.Puts),
		Hits:   atomic.LoadInt64(&sp.metrics.Hits),
		Misses: atomic.LoadInt64(&sp.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}

// atomicMetrics provides atomic operations for metrics
type atomicMetrics struct {
	Gets   int64
	Puts   int64
	Hits   int64
	Misses int64
}