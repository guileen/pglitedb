// Package pool provides object pooling functionality for PGLiteDB
package pool

import (
	"strings"
	"sync"
	"sync/atomic"
)

// StringBuilderPool is a specialized pool for strings.Builder objects
type StringBuilderPool struct {
	pool    *sync.Pool
	name    string
	metrics atomicMetrics
}

// NewStringBuilderPool creates a new StringBuilderPool
func NewStringBuilderPool(name string) *StringBuilderPool {
	return &StringBuilderPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return &strings.Builder{}
			},
		},
		name: name,
	}
}

// Get retrieves a strings.Builder from the pool
func (sbp *StringBuilderPool) Get() *strings.Builder {
	atomic.AddInt64(&sbp.metrics.Gets, 1)
	builder := sbp.pool.Get().(*strings.Builder)
	if builder == nil {
		atomic.AddInt64(&sbp.metrics.Misses, 1)
		// Create a new builder if the pooled one was nil
		builder = &strings.Builder{}
	} else {
		atomic.AddInt64(&sbp.metrics.Hits, 1)
		// Reset the builder for reuse
		builder.Reset()
	}
	return builder
}

// Put returns a strings.Builder to the pool
func (sbp *StringBuilderPool) Put(builder *strings.Builder) {
	atomic.AddInt64(&sbp.metrics.Puts, 1)
	// Only put non-nil builders back in the pool
	if builder != nil {
		// Reset the builder before putting it back
		builder.Reset()
		sbp.pool.Put(builder)
	}
}

// Metrics returns the current pool metrics
func (sbp *StringBuilderPool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&sbp.metrics.Gets),
		Puts:   atomic.LoadInt64(&sbp.metrics.Puts),
		Hits:   atomic.LoadInt64(&sbp.metrics.Hits),
		Misses: atomic.LoadInt64(&sbp.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}