package pools

import (
	"github.com/guileen/pglitedb/storage"
)

// BatchPool manages batch resources
type BatchPool struct {
	BasePool
}

// BatchWrapper wraps a storage batch for pooling
type BatchWrapper struct {
	batch storage.Batch
}

// NewBatchPool creates a new batch pool
func NewBatchPool() *BatchPool {
	return &BatchPool{
		BasePool: *NewBasePool("batch", func() interface{} {
			return &BatchWrapper{}
		}),
	}
}

// Acquire gets a batch from the pool
func (bp *BatchPool) Acquire() *BatchWrapper {
	batch := bp.BasePool.pool.Get()
	fromPool := batch != nil
	
	if !fromPool {
		batch = &BatchWrapper{}
	}
	
	return batch.(*BatchWrapper)
}

// Release returns a batch to the pool
func (bp *BatchPool) Release(batch *BatchWrapper) {
	batch.batch = nil
	bp.BasePool.Put(batch)
}