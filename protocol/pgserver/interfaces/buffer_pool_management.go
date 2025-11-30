package interfaces

// BufferPoolManagementInterface defines the interface for buffer pool management operations
type BufferPoolManagementInterface interface {
	GetBufferPool() interface{}
	SetBufferPool(bufferPool interface{})
	HealthCheck() error
}