// Package benchprof provides profiling utilities for Go benchmark tests
package benchprof

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/profiling"
)

// Profiling flags
var (
	enableCPUProfiling    = flag.Bool("cpuprofile", false, "Enable CPU profiling during benchmarks")
	enableMemProfiling    = flag.Bool("memprofile", false, "Enable memory profiling during benchmarks")
	enableAllocProfiling  = flag.Bool("allocprofile", false, "Enable allocation profiling during benchmarks")
	enableBlockProfiling  = flag.Bool("blockprofile", false, "Enable block profiling during benchmarks")
	enableMutexProfiling  = flag.Bool("mutexprofile", false, "Enable mutex profiling during benchmarks")
	profileOutputDir      = flag.String("profiledir", "./profiles", "Directory to save profile files")
	profilePrefix         = flag.String("profileprefix", "benchmark", "Prefix for profile filenames")
)

// Profiler wraps the profiling.Profiler with benchmark-specific functionality
type Profiler struct {
	profiler     *profiling.Profiler
	outputDir    string
	prefix       string
	testName     string
	startTime    time.Time
	cpuFile      string
	memFile      string
	allocFile    string
	blockFile    string
	mutexFile    string
}

// NewProfiler creates a new benchmark profiler
func NewProfiler() *Profiler {
	// Parse flags if not already parsed
	if !flag.Parsed() {
		flag.Parse()
	}

	return &Profiler{
		profiler:  profiling.NewProfiler(),
		outputDir: *profileOutputDir,
		prefix:    *profilePrefix,
	}
}

// StartBenchmark starts profiling for a benchmark test
func (bp *Profiler) StartBenchmark(b *testing.B) {
	bp.testName = b.Name()
	bp.startTime = time.Now()

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(bp.outputDir, 0755); err != nil {
		b.Fatalf("Failed to create profile output directory: %v", err)
	}

	// Generate timestamp for unique filenames
	timestamp := time.Now().Format("20060102_150405")
	baseName := fmt.Sprintf("%s_%s_%s", bp.prefix, bp.testName, timestamp)

	// Start CPU profiling if enabled
	if *enableCPUProfiling {
		bp.cpuFile = filepath.Join(bp.outputDir, baseName+"_cpu.prof")
		if err := bp.profiler.StartCPUProfile(bp.cpuFile); err != nil {
			b.Fatalf("Failed to start CPU profiling: %v", err)
		}
		b.Cleanup(func() {
			if bp.profiler.IsProfileActive(profiling.CPUProfile) {
				bp.profiler.StopCPUProfile()
			}
		})
	}

	// Start block profiling if enabled
	if *enableBlockProfiling {
		if err := bp.profiler.StartBlockProfile(1); err != nil {
			b.Fatalf("Failed to start block profiling: %v", err)
		}
		bp.blockFile = filepath.Join(bp.outputDir, baseName+"_block.prof")
		b.Cleanup(func() {
			if bp.profiler.IsProfileActive(profiling.BlockProfile) {
				bp.profiler.StopBlockProfile(bp.blockFile)
			}
		})
	}

	// Start mutex profiling if enabled
	if *enableMutexProfiling {
		if err := bp.profiler.StartMutexProfile(1); err != nil {
			b.Fatalf("Failed to start mutex profiling: %v", err)
		}
		bp.mutexFile = filepath.Join(bp.outputDir, baseName+"_mutex.prof")
		b.Cleanup(func() {
			if bp.profiler.IsProfileActive(profiling.MutexProfile) {
				bp.profiler.StopMutexProfile(bp.mutexFile)
			}
		})
	}

	// Start allocation profiling if enabled
	if *enableAllocProfiling {
		if err := bp.profiler.StartAllocProfile(""); err != nil {
			b.Fatalf("Failed to start allocation profiling: %v", err)
		}
		bp.allocFile = filepath.Join(bp.outputDir, baseName+"_allocs.prof")
		b.Cleanup(func() {
			if bp.profiler.IsProfileActive(profiling.AllocProfile) {
				bp.profiler.StopAllocProfile(bp.allocFile)
			}
		})
	}
}

// StopBenchmark stops profiling and saves memory profile if enabled
func (bp *Profiler) StopBenchmark(b *testing.B) {
	// Take memory profile at the end if enabled
	if *enableMemProfiling {
		memFile := filepath.Join(bp.outputDir, fmt.Sprintf("%s_%s_mem.prof", bp.prefix, bp.testName))
		if err := bp.profiler.StartMemProfile(memFile); err != nil {
			b.Fatalf("Failed to take memory profile: %v", err)
		}
	}

	// Print profiling summary
	duration := time.Since(bp.startTime)
	b.Logf("Benchmark %s completed in %v", bp.testName, duration)
	
	if *enableCPUProfiling {
		b.Logf("CPU profile saved to: %s", bp.cpuFile)
	}
	if *enableMemProfiling {
		b.Logf("Memory profile saved to: %s", filepath.Join(bp.outputDir, fmt.Sprintf("%s_%s_mem.prof", bp.prefix, bp.testName)))
	}
	if *enableAllocProfiling {
		b.Logf("Allocation profile will be saved to: %s", bp.allocFile)
	}
	if *enableBlockProfiling {
		b.Logf("Block profile will be saved to: %s", bp.blockFile)
	}
	if *enableMutexProfiling {
		b.Logf("Mutex profile will be saved to: %s", bp.mutexFile)
	}
}

// WithProfiling wraps a benchmark function with profiling
func WithProfiling(b *testing.B, benchmarkFunc func(*testing.B)) {
	profiler := NewProfiler()
	profiler.StartBenchmark(b)
	defer profiler.StopBenchmark(b)
	
	benchmarkFunc(b)
}

// WriteProfile writes a specific profile type to a file
func (bp *Profiler) WriteProfile(profileType profiling.ProfileType, filename string) error {
	switch profileType {
	case profiling.GoroutineProfile:
		return bp.profiler.WriteGoroutineProfile(filename)
	case profiling.ThreadProfile:
		return bp.profiler.WriteThreadProfile(filename)
	default:
		return fmt.Errorf("profile type %s not supported for direct writing", profileType)
	}
}