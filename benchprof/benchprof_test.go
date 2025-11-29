package benchprof

import (
	"testing"
)

func TestWithProfiling(t *testing.T) {
	// This is a simple test to verify the profiling wrapper compiles and runs
	// In practice, this would be run with go test -bench=.
}

func BenchmarkExample(b *testing.B) {
	WithProfiling(b, func(b *testing.B) {
		// Simple benchmark to test the profiling wrapper
		for i := 0; i < b.N; i++ {
			_ = i * i
		}
	})
}