package pools

import (
	scan "github.com/guileen/pglitedb/engine/pebble/operations/scan"
)

// IndexIteratorPool manages IndexIterator resources
type IndexIteratorPool struct {
	BasePool
}

// NewIndexIteratorPool creates a new index iterator pool
func NewIndexIteratorPool() *IndexIteratorPool {
	return &IndexIteratorPool{
		BasePool: *NewBasePool("indexIterator", func() interface{} {
			return &scan.IndexIterator{}
		}),
	}
}

// Acquire gets an IndexIterator from the pool
func (iip *IndexIteratorPool) Acquire() *scan.IndexIterator {
	iter := iip.BasePool.pool.Get()
	fromPool := iter != nil
	
	if !fromPool {
		iter = &scan.IndexIterator{}
	}
	
	return iter.(*scan.IndexIterator)
}

// Release returns an IndexIterator to the pool
func (iip *IndexIteratorPool) Release(iter *scan.IndexIterator) {
	if iter != nil {
		iter.ResetForReuse()
		iip.BasePool.Put(iter)
	}
}

// RowIteratorPool manages RowIterator resources
type RowIteratorPool struct {
	BasePool
}

// NewRowIteratorPool creates a new row iterator pool
func NewRowIteratorPool() *RowIteratorPool {
	return &RowIteratorPool{
		BasePool: *NewBasePool("rowIterator", func() interface{} {
			return &scan.RowIterator{}
		}),
	}
}

// Acquire gets a RowIterator from the pool
func (rip *RowIteratorPool) Acquire() *scan.RowIterator {
	iter := rip.BasePool.pool.Get()
	fromPool := iter != nil
	
	if !fromPool {
		iter = &scan.RowIterator{}
	}
	
	return iter.(*scan.RowIterator)
}

// Release returns a RowIterator to the pool
func (rip *RowIteratorPool) Release(iter *scan.RowIterator) {
	if iter != nil {
		iter.ResetForReuse()
		rip.BasePool.Put(iter)
	}
}

// IndexOnlyIteratorPool manages IndexOnlyIterator resources
type IndexOnlyIteratorPool struct {
	BasePool
}

// NewIndexOnlyIteratorPool creates a new index-only iterator pool
func NewIndexOnlyIteratorPool() *IndexOnlyIteratorPool {
	return &IndexOnlyIteratorPool{
		BasePool: *NewBasePool("indexOnlyIterator", func() interface{} {
			return &scan.IndexOnlyIterator{}
		}),
	}
}

// Acquire gets an IndexOnlyIterator from the pool
func (ioip *IndexOnlyIteratorPool) Acquire() *scan.IndexOnlyIterator {
	iter := ioip.BasePool.pool.Get()
	fromPool := iter != nil
	
	if !fromPool {
		iter = &scan.IndexOnlyIterator{}
	}
	
	return iter.(*scan.IndexOnlyIterator)
}

// Release returns an IndexOnlyIterator to the pool
func (ioip *IndexOnlyIteratorPool) Release(iter *scan.IndexOnlyIterator) {
	if iter != nil {
		iter.ResetForReuse()
		ioip.BasePool.Put(iter)
	}
}