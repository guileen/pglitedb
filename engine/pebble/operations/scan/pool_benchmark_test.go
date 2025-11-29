package scan

import (
	"testing"
)

// BenchmarkRowIteratorAllocation measures the cost of creating new RowIterator instances
func BenchmarkRowIteratorAllocation(b *testing.B) {
	b.ReportAllocs()
	
	for i := 0; i < b.N; i++ {
		// Simulate creating a new iterator each time (no pooling)
		iter := &RowIterator{}
		_ = iter
		// In real usage, this would be returned to a pool in Close()
	}
}

// BenchmarkRowIteratorReset measures the cost of resetting an existing RowIterator
func BenchmarkRowIteratorReset(b *testing.B) {
	b.ReportAllocs()
	
	// Create one iterator to reuse
	iter := &RowIterator{}
	
	for i := 0; i < b.N; i++ {
		// Simulate resetting an existing iterator (pooling)
		iter.Reset()
	}
}