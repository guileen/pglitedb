package pools

import (
	"testing"
	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
)

// BenchmarkIteratorPoolingWithManager tests the performance of iterator pooling with the pool manager
func BenchmarkIteratorPoolingWithManager(b *testing.B) {
	manager := NewManager(nil, nil)
	
	b.Run("AcquireAndReleaseIndexIteratorWithManager", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := manager.AcquireIndexIterator()
			manager.ReleaseIndexIterator(iter)
		}
	})
	
	b.Run("AcquireAndReleaseRowIteratorWithManager", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := manager.AcquireRowIterator()
			manager.ReleaseRowIterator(iter)
		}
	})
	
	b.Run("AcquireAndReleaseIndexOnlyIteratorWithManager", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := manager.AcquireIndexOnlyIterator()
			manager.ReleaseIndexOnlyIterator(iter)
		}
	})
}

// BenchmarkIteratorCreationDirect tests the performance of creating new iterators directly (no pooling)
func BenchmarkIteratorCreationDirect(b *testing.B) {
	b.Run("CreateNewIndexIteratorDirect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &scan.IndexIterator{}
			_ = iter
		}
	})
	
	b.Run("CreateNewRowIteratorDirect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &scan.RowIterator{}
			_ = iter
		}
	})
	
	b.Run("CreateNewIndexOnlyIteratorDirect", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			iter := &scan.IndexOnlyIterator{}
			_ = iter
		}
	})
}

// BenchmarkBufferPooling tests the performance of buffer pooling
func BenchmarkBufferPooling(b *testing.B) {
	bufferPool := NewBufferPool()
	
	b.Run("AcquireAndReleaseKeyBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := bufferPool.AcquireKeyBuffer()
			bufferPool.ReleaseKeyBuffer(buf)
		}
	})
	
	b.Run("AcquireAndReleaseValueBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := bufferPool.AcquireValueBuffer()
			bufferPool.ReleaseValueBuffer(buf)
		}
	})
	
	b.Run("AcquireAndReleaseRowIDBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := bufferPool.AcquireRowIDBuffer()
			bufferPool.ReleaseRowIDBuffer(buf)
		}
	})
}

// BenchmarkBufferCreation tests the performance of creating new buffers (no pooling)
func BenchmarkBufferCreation(b *testing.B) {
	b.Run("CreateNewKeyBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 0, 128)
			_ = buf
		}
	})
	
	b.Run("CreateNewValueBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]byte, 0, 256)
			_ = buf
		}
	})
	
	b.Run("CreateNewRowIDBuffer", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			buf := make([]int64, 0, 64)
			_ = buf
		}
	})
}