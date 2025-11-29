package scan

import (
	"testing"
)

func TestPooledIterators(t *testing.T) {
	// Create an iterator pool
	iteratorPool := NewIteratorPool()
	
	// Test PooledIndexIterator creation and closing
	indexIter := iteratorPool.AcquireIndexIterator()
	pooledIndexIter := NewPooledIndexIterator(indexIter, iteratorPool)
	if pooledIndexIter == nil {
		t.Fatal("Failed to create PooledIndexIterator")
	}
	
	// Close should return the iterator to the pool without error
	err := pooledIndexIter.Close()
	if err != nil {
		t.Errorf("Error closing PooledIndexIterator: %v", err)
	}
	
	// Test PooledRowIterator creation and closing
	rowIter := iteratorPool.AcquireRowIterator()
	pooledRowIter := NewPooledRowIterator(rowIter, iteratorPool)
	if pooledRowIter == nil {
		t.Fatal("Failed to create PooledRowIterator")
	}
	
	// Close should return the iterator to the pool without error
	err = pooledRowIter.Close()
	if err != nil {
		t.Errorf("Error closing PooledRowIterator: %v", err)
	}
	
	// Test PooledIndexOnlyIterator creation and closing
	indexOnlyIter := iteratorPool.AcquireIndexOnlyIterator()
	pooledIndexOnlyIter := NewPooledIndexOnlyIterator(indexOnlyIter, iteratorPool)
	if pooledIndexOnlyIter == nil {
		t.Fatal("Failed to create PooledIndexOnlyIterator")
	}
	
	// Close should return the iterator to the pool without error
	err = pooledIndexOnlyIter.Close()
	if err != nil {
		t.Errorf("Error closing PooledIndexOnlyIterator: %v", err)
	}
	
	t.Log("Pooled iterators test completed successfully")
}