package scan

import (
	"testing"
)

func TestIteratorPooling(t *testing.T) {
	// Create an iterator pool
	iteratorPool := NewIteratorPool()
	
	// Acquire and release several iterators to test pooling
	for i := 0; i < 10; i++ {
		// Test IndexIterator pooling
		indexIter := iteratorPool.AcquireIndexIterator()
		if indexIter == nil {
			t.Fatal("Failed to acquire IndexIterator from pool")
		}
		iteratorPool.ReleaseIndexIterator(indexIter)
		
		// Test RowIterator pooling
		rowIter := iteratorPool.AcquireRowIterator()
		if rowIter == nil {
			t.Fatal("Failed to acquire RowIterator from pool")
		}
		iteratorPool.ReleaseRowIterator(rowIter)
		
		// Test IndexOnlyIterator pooling
		indexOnlyIter := iteratorPool.AcquireIndexOnlyIterator()
		if indexOnlyIter == nil {
			t.Fatal("Failed to acquire IndexOnlyIterator from pool")
		}
		iteratorPool.ReleaseIndexOnlyIterator(indexOnlyIter)
	}
	
	t.Log("Iterator pooling test completed successfully")
}