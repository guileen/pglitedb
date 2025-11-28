package codec

import (
	"testing"
)

func BenchmarkEncodeTableKeyOriginal(b *testing.B) {
	c := NewMemComparableCodec()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = c.EncodeTableKey(1, int64(i), 3)
	}
}

func BenchmarkEncodeTableKeyBuffer(b *testing.B) {
	c := NewMemComparableCodec()
	buf := make([]byte, 0, 32)
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.EncodeTableKeyBuffer(1, int64(i), 3, buf)
	}
}

func BenchmarkEncodeTableKeyBufferNil(b *testing.B) {
	c := NewMemComparableCodec()
	
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.EncodeTableKeyBuffer(1, int64(i), 3, nil)
	}
}