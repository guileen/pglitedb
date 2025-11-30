package buffer

import (
	"sync"

	"github.com/guileen/pglitedb/pool"
)

// BufferPoolManager handles buffer pool management
type BufferPoolManager struct {
	bufferPool *pool.MultiBufferPool
	mu         sync.RWMutex
}

// NewBufferPoolManager creates a new buffer pool manager
func NewBufferPoolManager() *BufferPoolManager {
	return &BufferPoolManager{}
}

// InitializeBufferPool initializes the buffer pool with the given name and sizes
func (bpm *BufferPoolManager) InitializeBufferPool(name string, sizes []int) error {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()

	bpm.bufferPool = pool.NewMultiBufferPool(name, sizes)
	return nil
}

// GetBuffer retrieves a byte slice from the appropriate pool
func (bpm *BufferPoolManager) GetBuffer(size int) []byte {
	bpm.mu.RLock()
	pool := bpm.bufferPool
	bpm.mu.RUnlock()

	if pool == nil {
		// If no pool is initialized, create a new buffer directly
		return make([]byte, size)
	}

	return pool.Get(size)
}

// PutBuffer returns a byte slice to the appropriate pool
func (bpm *BufferPoolManager) PutBuffer(buf []byte) {
	bpm.mu.RLock()
	pool := bpm.bufferPool
	bpm.mu.RUnlock()

	if pool != nil && buf != nil {
		pool.Put(buf)
	}
}

// GetBufferPool returns the current buffer pool
func (bpm *BufferPoolManager) GetBufferPool() *pool.MultiBufferPool {
	bpm.mu.RLock()
	defer bpm.mu.RUnlock()
	return bpm.bufferPool
}

// SetBufferPool sets the buffer pool
func (bpm *BufferPoolManager) SetBufferPool(bufferPool *pool.MultiBufferPool) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bpm.bufferPool = bufferPool
}

// GetPoolMetrics returns the combined metrics for all pools
func (bpm *BufferPoolManager) GetPoolMetrics() pool.PoolMetrics {
	bpm.mu.RLock()
	bufferPool := bpm.bufferPool
	bpm.mu.RUnlock()

	if bufferPool == nil {
		return pool.PoolMetrics{}
	}

	return bufferPool.Metrics()
}

// HealthCheck performs a health check on the buffer pool manager
func (bpm *BufferPoolManager) HealthCheck() error {
	// Simple health check implementation
	return nil
}