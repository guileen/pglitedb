package pools

import (
	"sync"
	"sync/atomic"
)

// Pool represents a generic resource pool interface
type Pool interface {
	Get() (interface{}, bool)
	Put(interface{})
	Name() string
}

// BasePool provides common functionality for all pools
type BasePool struct {
	name     string
	pool     sync.Pool
	acquired uint64
	released uint64
	created  uint64
	errors   uint64
}

// NewBasePool creates a new base pool
func NewBasePool(name string, factory func() interface{}) *BasePool {
	return &BasePool{
		name: name,
		pool: sync.Pool{
			New: factory,
		},
	}
}

// Get retrieves a resource from the pool
func (bp *BasePool) Get() (interface{}, bool) {
	resource := bp.pool.Get()
	fromPool := resource != nil
	
	if !fromPool {
		atomic.AddUint64(&bp.created, 1)
		resource = bp.pool.New()
	}
	
	atomic.AddUint64(&bp.acquired, 1)
	return resource, fromPool
}

// Put returns a resource to the pool
func (bp *BasePool) Put(resource interface{}) {
	atomic.AddUint64(&bp.released, 1)
	bp.pool.Put(resource)
}

// IncrementErrors atomically increments the error counter
func (bp *BasePool) IncrementErrors() {
	atomic.AddUint64(&bp.errors, 1)
}

// Name returns the pool name
func (bp *BasePool) Name() string {
	return bp.name
}

// Stats returns pool statistics
func (bp *BasePool) Stats() PoolStats {
	return PoolStats{
		Name:     bp.name,
		Acquired: atomic.LoadUint64(&bp.acquired),
		Released: atomic.LoadUint64(&bp.released),
		Created:  atomic.LoadUint64(&bp.created),
		Errors:   atomic.LoadUint64(&bp.errors),
	}
}

// PoolStats represents statistics for a pool
type PoolStats struct {
	Name     string
	Acquired uint64
	Released uint64
	Created  uint64
	Errors   uint64
}