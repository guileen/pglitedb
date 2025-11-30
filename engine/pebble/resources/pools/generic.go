package pools

import (
	"sync"
)

// GenericPool provides a generic object pool implementation
type GenericPool struct {
	BasePool
}

// NewGenericPool creates a new generic pool
func NewGenericPool(name string, factory func() interface{}) *GenericPool {
	return &GenericPool{
		BasePool: *NewBasePool(name, factory),
	}
}

// Acquire gets an object from the pool
func (gp *GenericPool) Acquire() interface{} {
	obj := gp.BasePool.pool.Get()
	fromPool := obj != nil
	
	if !fromPool {
		obj = gp.BasePool.pool.New()
	}
	
	return obj
}

// Release returns an object to the pool
func (gp *GenericPool) Release(obj interface{}) {
	gp.BasePool.Put(obj)
}

// GenericPoolManager manages multiple generic pools
type GenericPoolManager struct {
	pools map[string]*GenericPool
	mutex sync.RWMutex
}

// NewGenericPoolManager creates a new generic pool manager
func NewGenericPoolManager() *GenericPoolManager {
	return &GenericPoolManager{
		pools: make(map[string]*GenericPool),
	}
}

// RegisterPool registers a new pool with the manager
func (gpm *GenericPoolManager) RegisterPool(name string, factory func() interface{}) {
	gpm.mutex.Lock()
	defer gpm.mutex.Unlock()
	
	gpm.pools[name] = NewGenericPool(name, factory)
}

// Acquire gets an object from the specified pool
func (gpm *GenericPoolManager) Acquire(name string) interface{} {
	gpm.mutex.RLock()
	pool, exists := gpm.pools[name]
	gpm.mutex.RUnlock()
	
	if !exists {
		return nil
	}
	
	return pool.Acquire()
}

// Release returns an object to the specified pool
func (gpm *GenericPoolManager) Release(name string, obj interface{}) {
	gpm.mutex.RLock()
	pool, exists := gpm.pools[name]
	gpm.mutex.RUnlock()
	
	if !exists {
		return
	}
	
	pool.Release(obj)
}