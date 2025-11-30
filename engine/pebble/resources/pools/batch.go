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

// BatchSlicePool manages slices of batches
type BatchSlicePool struct {
	BasePool
}

// NewBatchSlicePool creates a new batch slice pool
func NewBatchSlicePool() *BatchSlicePool {
	return &BatchSlicePool{
		BasePool: *NewBasePool("batchSlice", func() interface{} {
			return make([]storage.Batch, 0, 8) // Start with reasonable capacity
		}),
	}
}

// AcquireBatchSlice gets a batch slice from the pool
func (bsp *BatchSlicePool) AcquireBatchSlice() []storage.Batch {
	slice := bsp.BasePool.pool.Get()
	fromPool := slice != nil

	if !fromPool {
		return make([]storage.Batch, 0, 8)
	}

	return slice.([]storage.Batch)
}

// ReleaseBatchSlice returns a batch slice to the pool
func (bsp *BatchSlicePool) ReleaseBatchSlice(slice []storage.Batch) {
	// Clear the slice without reallocating
	for i := range slice {
		slice[i] = nil
	}
	slice = slice[:0]
	bsp.BasePool.Put(slice)
}