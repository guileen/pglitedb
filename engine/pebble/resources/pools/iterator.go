package pools

import (
	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
)

// IteratorPool manages iterator resources
type IteratorPool struct {
	BasePool
	leakDetector *leak.Detector
}

// NewIteratorPool creates a new iterator pool
func NewIteratorPool(leakDetector *leak.Detector) *IteratorPool {
	return &IteratorPool{
		BasePool: *NewBasePool("iterator", func() interface{} {
			return &scan.RowIterator{}
		}),
		leakDetector: leakDetector,
	}
}

// Acquire gets an iterator from the pool
func (ip *IteratorPool) Acquire() *scan.RowIterator {
	iter := ip.BasePool.pool.Get()
	fromPool := iter != nil

	if !fromPool {
		iter = &scan.RowIterator{}
	}

	// Track iterator for leak detection
	rowIter := iter.(*scan.RowIterator)
	if ip.leakDetector != nil {
		// Use the leak detector's GetLeakDetector() method to access the actual leak detector
		if ld := ip.leakDetector.GetLeakDetector(); ld != nil {
			stackTrace := "iterator acquisition stack trace" // In a real implementation, this would be an actual stack trace
			ld.TrackIterator(rowIter, stackTrace)
		}
	}

	return rowIter
}

// Release returns an iterator to the pool
func (ip *IteratorPool) Release(iter *scan.RowIterator) {
	// Reset the iterator state
	iter.Reset()

	ip.BasePool.Put(iter)
}