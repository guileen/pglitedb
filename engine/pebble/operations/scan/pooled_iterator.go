package scan

import (
	dbTypes "github.com/guileen/pglitedb/types"
)

// PooledIndexIterator wraps an IndexIterator and returns it to the pool when closed
type PooledIndexIterator struct {
	iter *IndexIterator
	pool *IteratorPool
}

// NewPooledIndexIterator creates a new PooledIndexIterator
func NewPooledIndexIterator(iter *IndexIterator, pool *IteratorPool) *PooledIndexIterator {
	return &PooledIndexIterator{
		iter: iter,
		pool: pool,
	}
}

func (pii *PooledIndexIterator) Next() bool {
	return pii.iter.Next()
}

func (pii *PooledIndexIterator) Row() *dbTypes.Record {
	return pii.iter.Row()
}

func (pii *PooledIndexIterator) Error() error {
	return pii.iter.Error()
}

func (pii *PooledIndexIterator) Close() error {
	// Only close the underlying iterator if it's not nil
	if pii.iter != nil {
		_ = pii.iter.Close()
	}
	pii.pool.ReleaseIndexIterator(pii.iter)
	return nil
}

// PooledRowIterator wraps a RowIterator and returns it to the pool when closed
type PooledRowIterator struct {
	iter *RowIterator
	pool *IteratorPool
}

// NewPooledRowIterator creates a new PooledRowIterator
func NewPooledRowIterator(iter *RowIterator, pool *IteratorPool) *PooledRowIterator {
	return &PooledRowIterator{
		iter: iter,
		pool: pool,
	}
}

func (pri *PooledRowIterator) Next() bool {
	return pri.iter.Next()
}

func (pri *PooledRowIterator) Row() *dbTypes.Record {
	return pri.iter.Row()
}

func (pri *PooledRowIterator) Error() error {
	return pri.iter.Error()
}

func (pri *PooledRowIterator) Close() error {
	// Only close the underlying iterator if it's not nil
	if pri.iter != nil {
		_ = pri.iter.Close()
	}
	pri.pool.ReleaseRowIterator(pri.iter)
	return nil
}

// PooledIndexOnlyIterator wraps an IndexOnlyIterator and returns it to the pool when closed
type PooledIndexOnlyIterator struct {
	iter *IndexOnlyIterator
	pool *IteratorPool
}

// NewPooledIndexOnlyIterator creates a new PooledIndexOnlyIterator
func NewPooledIndexOnlyIterator(iter *IndexOnlyIterator, pool *IteratorPool) *PooledIndexOnlyIterator {
	return &PooledIndexOnlyIterator{
		iter: iter,
		pool: pool,
	}
}

func (pioi *PooledIndexOnlyIterator) Next() bool {
	return pioi.iter.Next()
}

func (pioi *PooledIndexOnlyIterator) Row() *dbTypes.Record {
	return pioi.iter.Row()
}

func (pioi *PooledIndexOnlyIterator) Error() error {
	return pioi.iter.Error()
}

func (pioi *PooledIndexOnlyIterator) Close() error {
	// Only close the underlying iterator if it's not nil
	if pioi.iter != nil {
		_ = pioi.iter.Close()
	}
	pioi.pool.ReleaseIndexOnlyIterator(pioi.iter)
	return nil
}

// IteratorPool manages iterator resources with minimal overhead
type IteratorPool struct {
	indexIteratorPool    chan *IndexIterator
	rowIteratorPool      chan *RowIterator
	indexOnlyIteratorPool chan *IndexOnlyIterator
}

// NewIteratorPool creates a new iterator pool
func NewIteratorPool() *IteratorPool {
	return &IteratorPool{
		indexIteratorPool:    make(chan *IndexIterator, 100),
		rowIteratorPool:      make(chan *RowIterator, 100),
		indexOnlyIteratorPool: make(chan *IndexOnlyIterator, 100),
	}
}

// AcquireIndexIterator gets an IndexIterator from the pool
func (ip *IteratorPool) AcquireIndexIterator() *IndexIterator {
	select {
	case iter := <-ip.indexIteratorPool:
		return iter
	default:
		return &IndexIterator{}
	}
}

// ReleaseIndexIterator returns an IndexIterator to the pool
func (ip *IteratorPool) ReleaseIndexIterator(iter *IndexIterator) {
	// Reset the iterator state to avoid retaining references
	iter.ResetForReuse()
	
	select {
	case ip.indexIteratorPool <- iter:
	default:
		// Pool is full, discard the iterator
	}
}

// AcquireRowIterator gets a RowIterator from the pool
func (ip *IteratorPool) AcquireRowIterator() *RowIterator {
	select {
	case iter := <-ip.rowIteratorPool:
		return iter
	default:
		return &RowIterator{}
	}
}

// ReleaseRowIterator returns a RowIterator to the pool
func (ip *IteratorPool) ReleaseRowIterator(iter *RowIterator) {
	// Reset the iterator state to avoid retaining references
	iter.ResetForReuse()
	
	select {
	case ip.rowIteratorPool <- iter:
	default:
		// Pool is full, discard the iterator
	}
}

// AcquireIndexOnlyIterator gets an IndexOnlyIterator from the pool
func (ip *IteratorPool) AcquireIndexOnlyIterator() *IndexOnlyIterator {
	select {
	case iter := <-ip.indexOnlyIteratorPool:
		return iter
	default:
		return &IndexOnlyIterator{}
	}
}

// ReleaseIndexOnlyIterator returns an IndexOnlyIterator to the pool
func (ip *IteratorPool) ReleaseIndexOnlyIterator(iter *IndexOnlyIterator) {
	// Reset the iterator state to avoid retaining references
	iter.ResetForReuse()
	
	select {
	case ip.indexOnlyIteratorPool <- iter:
	default:
		// Pool is full, discard the iterator
	}
}