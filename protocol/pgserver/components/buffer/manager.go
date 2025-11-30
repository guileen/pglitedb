package buffer

import (
	"sync"

	"github.com/guileen/pglitedb/pool"
)

// BufferPoolManager handles buffer pool management
type BufferPoolManager struct {
	bufferPool interface{} // This would be *pool.MultiBufferPool
	mu         sync.Mutex
}

// NewBufferPoolManager creates a new buffer pool manager
func NewBufferPoolManager(bufferPool interface{}) *BufferPoolManager {
	return &BufferPoolManager{
		bufferPool: bufferPool,
	}
}

// GetBufferPool returns the current buffer pool
func (bpm *BufferPoolManager) GetBufferPool() interface{} {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	return bpm.bufferPool
}

// SetBufferPool sets the buffer pool
func (bpm *BufferPoolManager) SetBufferPool(bufferPool interface{}) {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bpm.bufferPool = bufferPool
}

// CreateBufferPool creates a new buffer pool with the specified sizes
func (bpm *BufferPoolManager) CreateBufferPool(name string, sizes []int) interface{} {
	bpm.mu.Lock()
	defer bpm.mu.Unlock()
	bufferPool := pool.NewMultiBufferPool(name, sizes)
	bpm.bufferPool = bufferPool
	return bufferPool
}

// HealthCheck performs a health check on the buffer pool manager
func (bpm *BufferPoolManager) HealthCheck() error {
	// Simple health check implementation
	// In a real implementation, this would check the buffer pool health
	return nil
}