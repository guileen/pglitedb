package pools

// BatchOperationPool manages batch operation resources
type BatchOperationPool struct {
	BasePool
}

// BatchOperationWrapper wraps a batch operation for pooling
type BatchOperationWrapper struct {
	RowIDs      []int64
	UpdatedRows map[int64]*interface{}
}

// NewBatchOperationPool creates a new batch operation pool
func NewBatchOperationPool() *BatchOperationPool {
	return &BatchOperationPool{
		BasePool: *NewBasePool("batchOperation", func() interface{} {
			return &BatchOperationWrapper{
				RowIDs:      make([]int64, 0, 64),
				UpdatedRows: make(map[int64]*interface{}),
			}
		}),
	}
}

// Acquire gets a batch operation from the pool
func (bop *BatchOperationPool) Acquire() *BatchOperationWrapper {
	op := bop.BasePool.pool.Get()
	fromPool := op != nil
	
	if !fromPool {
		op = &BatchOperationWrapper{
			RowIDs:      make([]int64, 0, 64),
			UpdatedRows: make(map[int64]*interface{}),
		}
	}
	
	return op.(*BatchOperationWrapper)
}

// Release returns a batch operation to the pool
func (bop *BatchOperationPool) Release(op *BatchOperationWrapper) {
	if op != nil {
		// Reset the operation state
		op.RowIDs = op.RowIDs[:0]
		
		// Clear the map without reallocating
		for k := range op.UpdatedRows {
			delete(op.UpdatedRows, k)
		}
		
		bop.BasePool.Put(op)
	}
}