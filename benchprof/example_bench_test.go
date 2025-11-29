// Package benchprof demonstrates how to use the benchprof package
package benchprof_test

import (
	"testing"

	"github.com/guileen/pglitedb/benchprof"
)

// BenchmarkExample demonstrates how to use benchprof with a simple benchmark
func BenchmarkExample(b *testing.B) {
	benchprof.WithProfiling(b, func(b *testing.B) {
		// Your benchmark code here
		for i := 0; i < b.N; i++ {
			// ... benchmarked code ...
			_ = i * i
		}
	})
}

// BenchmarkManualProfiling demonstrates how to manually control profiling
func BenchmarkManualProfiling(b *testing.B) {
	profiler := benchprof.NewProfiler()
	profiler.StartBenchmark(b)
	defer profiler.StopBenchmark(b)

	// Your benchmark code here
	for i := 0; i < b.N; i++ {
		// ... benchmarked code ...
		_ = i * i
	}
}