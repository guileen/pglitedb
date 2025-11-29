package scan

import (
	"testing"
)

// BenchmarkIteratorPooling tests the performance of iterator pooling
func BenchmarkIteratorPooling(b *testing.B) {
	iteratorPool := NewIteratorPool()
	
	b.Run("AcquireAndReleaseIndexIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := iteratorPool.AcquireIndexIterator()
			iteratorPool.ReleaseIndexIterator(iter)
		}
	})
	
	b.Run("AcquireAndReleaseRowIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := iteratorPool.AcquireRowIterator()
			iteratorPool.ReleaseRowIterator(iter)
		}
	})
	
	b.Run("AcquireAndReleaseIndexOnlyIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := iteratorPool.AcquireIndexOnlyIterator()
			iteratorPool.ReleaseIndexOnlyIterator(iter)
		}
	})
}

// BenchmarkIteratorCreation tests the performance of creating new iterators (no pooling)
func BenchmarkIteratorCreation(b *testing.B) {
	b.Run("CreateNewIndexIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &IndexIterator{}
			_ = iter
		}
	})
	
	b.Run("CreateNewRowIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &RowIterator{}
			_ = iter
		}
	})
	
	b.Run("CreateNewIndexOnlyIterator", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &IndexOnlyIterator{}
			_ = iter
		}
	})
}