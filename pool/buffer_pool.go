// Package pool provides object pooling functionality for PGLiteDB
package pool

import (
	"sync"
	"sync/atomic"
)

// BufferPool is a specialized pool for byte slices used in network I/O
type BufferPool struct {
	pool    *sync.Pool
	size    int
	name    string
	metrics atomicMetrics
}

// NewBufferPool creates a new BufferPool for byte slices of a specific size
func NewBufferPool(name string, size int) *BufferPool {
	return &BufferPool{
		pool: &sync.Pool{
			New: func() interface{} {
				return make([]byte, size)
			},
		},
		size: size,
		name: name,
	}
}

// Get retrieves a byte slice from the pool
func (bp *BufferPool) Get() []byte {
	atomic.AddInt64(&bp.metrics.Gets, 1)
	buf := bp.pool.Get().([]byte)
	if len(buf) == 0 {
		atomic.AddInt64(&bp.metrics.Misses, 1)
		// Create a new buffer if the pooled one was empty
		buf = make([]byte, bp.size)
	} else {
		atomic.AddInt64(&bp.metrics.Hits, 1)
	}
	return buf
}

// Put returns a byte slice to the pool
func (bp *BufferPool) Put(buf []byte) {
	atomic.AddInt64(&bp.metrics.Puts, 1)
	// Only put buffers of the correct size back in the pool
	if cap(buf) >= bp.size {
		// Reset the buffer to its full capacity
		buf = buf[:bp.size]
		bp.pool.Put(buf)
	}
	// Buffers that are too small are discarded
}

// Metrics returns the current pool metrics
func (bp *BufferPool) Metrics() PoolMetrics {
	return PoolMetrics{
		Gets:   atomic.LoadInt64(&bp.metrics.Gets),
		Puts:   atomic.LoadInt64(&bp.metrics.Puts),
		Hits:   atomic.LoadInt64(&bp.metrics.Hits),
		Misses: atomic.LoadInt64(&bp.metrics.Misses),
		Size:   int64(0), // sync.Pool doesn't expose size
	}
}

// MultiBufferPool manages multiple buffer pools for different sizes
type MultiBufferPool struct {
	pools   map[int]*BufferPool
	sizes   []int
	name    string
	metrics atomicMetrics
	mu      sync.RWMutex
}

// NewMultiBufferPool creates a new MultiBufferPool
func NewMultiBufferPool(name string, sizes []int) *MultiBufferPool {
	pools := make(map[int]*BufferPool)
	for _, size := range sizes {
		pools[size] = NewBufferPool(name, size)
	}
	
	// Sort sizes for efficient lookup
	sortedSizes := make([]int, len(sizes))
	copy(sortedSizes, sizes)
	
	// Simple bubble sort for small arrays
	for i := 0; i < len(sortedSizes)-1; i++ {
		for j := 0; j < len(sortedSizes)-i-1; j++ {
			if sortedSizes[j] > sortedSizes[j+1] {
				sortedSizes[j], sortedSizes[j+1] = sortedSizes[j+1], sortedSizes[j]
			}
		}
	}
	
	return &MultiBufferPool{
		pools: pools,
		sizes: sortedSizes,
		name:  name,
	}
}

// Get retrieves a byte slice from the appropriate pool
func (mbp *MultiBufferPool) Get(size int) []byte {
	// Note: We don't increment metrics here because they are incremented
	// in the underlying pool's Get method
	
	// Find the smallest pool that can accommodate the requested size
	pool := mbp.getPoolForSize(size)
	if pool != nil {
		buf := pool.Get()
		return buf
	}
	
	// If no suitable pool, create a new buffer
	atomic.AddInt64(&mbp.metrics.Misses, 1)
	atomic.AddInt64(&mbp.metrics.Gets, 1)
	return make([]byte, size)
}

// Put returns a byte slice to the appropriate pool
func (mbp *MultiBufferPool) Put(buf []byte) {
	// Note: We don't increment metrics here because they are incremented
	// in the underlying pool's Put method
	
	// Find the pool that matches the buffer's capacity
	pool := mbp.pools[cap(buf)]
	if pool != nil {
		pool.Put(buf)
	}
	// Buffers that don't match any pool are discarded
}

// getPoolForSize finds the smallest pool that can accommodate the requested size
func (mbp *MultiBufferPool) getPoolForSize(size int) *BufferPool {
	mbp.mu.RLock()
	defer mbp.mu.RUnlock()
	
	// Binary search for the smallest pool that can accommodate the size
	for _, poolSize := range mbp.sizes {
		if poolSize >= size {
			return mbp.pools[poolSize]
		}
	}
	return nil
}

// Metrics returns the combined metrics for all pools
func (mbp *MultiBufferPool) Metrics() PoolMetrics {
	mbp.mu.RLock()
	defer mbp.mu.RUnlock()
	
	totalMetrics := PoolMetrics{}
	for _, pool := range mbp.pools {
		metrics := pool.Metrics()
		totalMetrics.Gets += metrics.Gets
		totalMetrics.Puts += metrics.Puts
		totalMetrics.Hits += metrics.Hits
		totalMetrics.Misses += metrics.Misses
	}
	
	return totalMetrics
}