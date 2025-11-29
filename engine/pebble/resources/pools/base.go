package pools

import (
	"sync"
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
		bp.created++
		resource = bp.pool.New()
	}
	
	bp.acquired++
	return resource, fromPool
}

// Put returns a resource to the pool
func (bp *BasePool) Put(resource interface{}) {
	bp.released++
	bp.pool.Put(resource)
}

// Name returns the pool name
func (bp *BasePool) Name() string {
	return bp.name
}

// Stats returns pool statistics
func (bp *BasePool) Stats() PoolStats {
	return PoolStats{
		Name:     bp.name,
		Acquired: bp.acquired,
		Released: bp.released,
		Created:  bp.created,
		Errors:   bp.errors,
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