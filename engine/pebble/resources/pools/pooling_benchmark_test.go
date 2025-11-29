package pools

import (
	"testing"

	"github.com/guileen/pglitedb/engine/pebble/operations/scan"
	"github.com/guileen/pglitedb/engine/pebble/resources/leak"
)

func BenchmarkIterator_WithPooling(b *testing.B) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		iter := poolManager.AcquireIterator()
		// Simulate some minimal work
		iter.Reset()
		poolManager.ReleaseIterator(iter)
	}
}

func BenchmarkIterator_WithoutPooling(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		iter := &scan.RowIterator{}
		// Simulate some minimal work
		iter.Reset()
		// iter goes out of scope and will be garbage collected
		_ = iter
	}
}

func BenchmarkKeyBuffer_Pooled(b *testing.B) {
	leakDetector := leak.NewDetector()
	poolManager := NewManager(leakDetector, nil)
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		buf := poolManager.AcquireIndexKeyBuffer(256)
		// Simulate some work with the buffer
		for j := 0; j < 100 && j < cap(buf); j++ {
			buf = append(buf, byte(j%256))
		}
		poolManager.ReleaseIndexKeyBuffer(buf)
	}
}

func BenchmarkKeyBuffer_Unpooled(b *testing.B) {
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		buf := make([]byte, 0, 256)
		// Simulate some work with the buffer
		for j := 0; j < 100; j++ {
			buf = append(buf, byte(j%256))
		}
		// buf goes out of scope and will be garbage collected
		_ = buf
	}
}