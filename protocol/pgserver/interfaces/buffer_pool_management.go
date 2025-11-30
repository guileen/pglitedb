package interfaces

import (
	"github.com/guileen/pglitedb/pool"
)

// BufferPoolManagementInterface defines the interface for buffer pool management operations
type BufferPoolManagementInterface interface {
	InitializeBufferPool(name string, sizes []int) error
	GetBuffer(size int) []byte
	PutBuffer(buf []byte)
	GetBufferPool() *pool.MultiBufferPool
	SetBufferPool(bufferPool *pool.MultiBufferPool)
	GetPoolMetrics() pool.PoolMetrics
	HealthCheck() error
}