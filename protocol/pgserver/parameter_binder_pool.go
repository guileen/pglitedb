// Package pgserver provides PostgreSQL server implementation for PGLiteDB
package pgserver

import (
	"sync"
	"sync/atomic"

	pg_query "github.com/pganalyze/pg_query_go/v6"
)

// ParameterBinderPool is a specialized pool for ParameterBinder objects
type ParameterBinderPool struct {
	pool    *sync.Pool
	metrics atomicMetrics
}

// NewParameterBinderPool creates a new ParameterBinderPool
func NewParameterBinderPool() *ParameterBinderPool {
	return &ParameterBinderPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &ParameterBinder{} // We'll need to modify ParameterBinder to be poolable
			},
		},
	}
}

// Get retrieves a ParameterBinder from the pool
func (p *ParameterBinderPool) Get(ast *pg_query.ParseResult, params []interface{}) *ParameterBinder {
	atomic.AddInt64(&p.metrics.Gets, 1)
	binder := p.pool.Get().(*ParameterBinder)
	if binder == nil {
		atomic.AddInt64(&p.metrics.Misses, 1)
		return NewParameterBinder(ast, params)
	}
	atomic.AddInt64(&p.metrics.Hits, 1)
	// Initialize the binder with the provided parameters
	binder.Init(ast, params)
	return binder
}

// Put returns a ParameterBinder to the pool
func (p *ParameterBinderPool) Put(binder *ParameterBinder) {
	if binder == nil {
		return
	}
	atomic.AddInt64(&p.metrics.Puts, 1)
	// Reset the binder before putting it back
	binder.Reset()
	p.pool.Put(binder)
}

// Metrics returns the current pool metrics
func (p *ParameterBinderPool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&p.metrics.Gets),
		Puts:   atomic.LoadInt64(&p.metrics.Puts),
		Hits:   atomic.LoadInt64(&p.metrics.Hits),
		Misses: atomic.LoadInt64(&p.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}

// PoolMetrics tracks pool performance and health
type PoolMetrics struct {
	Gets      int64 // Number of Get() operations
	Puts      int64 // Number of Put() operations
	Hits      int64 // Number of successful reuse operations
	Misses    int64 // Number of allocations due to pool miss
	Size      int64 // Current pool size
}

// atomicMetrics provides atomic operations for metrics
type atomicMetrics struct {
	Gets   int64
	Puts   int64
	Hits   int64
	Misses int64
}