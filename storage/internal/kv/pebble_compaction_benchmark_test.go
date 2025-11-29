package kv

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

func BenchmarkPebbleCompaction_DefaultConfig(b *testing.B) {
	benchmarkPebbleCompaction(b, func(path string) *PebbleConfig {
		return DefaultPebbleConfig(path)
	})
}

func BenchmarkPebbleCompaction_PostgreSQLConfig(b *testing.B) {
	benchmarkPebbleCompaction(b, func(path string) *PebbleConfig {
		return PostgreSQLOptimizedPebbleConfig(path)
	})
}

func BenchmarkPebbleCompaction_SpaceOptimizedConfig(b *testing.B) {
	benchmarkPebbleCompaction(b, func(path string) *PebbleConfig {
		return SpaceOptimizedPebbleConfig(path)
	})
}

func benchmarkPebbleCompaction(b *testing.B, configFactory func(string) *PebbleConfig) {
	tmpDir, err := os.MkdirTemp("", "pebble-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := configFactory(filepath.Join(tmpDir, "db"))
	kvStore, err := NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create kv store: %v", err)
	}
	defer kvStore.Close()

	ctx := context.Background()

	// Pre-populate with initial data
	initialData := 10000
	b.Logf("Pre-populating with %d records", initialData)
	
	for i := 0; i < initialData; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		value := []byte(fmt.Sprintf("value-%08d-data-content-here", i))
		if err := kvStore.Set(ctx, key, value); err != nil {
			b.Fatalf("set key %d: %v", i, err)
		}
	}

	// Force a flush to trigger compaction
	if err := kvStore.Flush(); err != nil {
		b.Fatalf("flush: %v", err)
	}

	// Wait a bit for initial compaction
	time.Sleep(100 * time.Millisecond)

	b.ResetTimer()

	// Perform mixed read/write workload
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// 70% reads, 30% writes to simulate typical PostgreSQL workload
			if i%10 < 7 {
				// Read operation
				key := []byte(fmt.Sprintf("key-%08d", i%initialData))
				_, err := kvStore.Get(ctx, key)
				if err != nil && err != shared.ErrNotFound {
					b.Errorf("get key %d: %v", i, err)
				}
			} else {
				// Write operation
				key := []byte(fmt.Sprintf("key-%08d-new", i))
				value := []byte(fmt.Sprintf("value-%08d-new-data-content-here", i))
				if err := kvStore.Set(ctx, key, value); err != nil {
					b.Errorf("set key %d: %v", i, err)
				}
			}
			i++
		}
	})

	b.StopTimer()
	
	// Collect final stats
	stats := kvStore.Stats()
	b.ReportMetric(float64(stats.ReadAmplification), "read-amp")
	b.ReportMetric(stats.WriteAmplification, "write-amp")
	b.ReportMetric(stats.SpaceAmplification, "space-amp")
	b.ReportMetric(float64(stats.L0FileCount), "l0-files")
	b.ReportMetric(float64(stats.L1FileCount), "l1-files")
	b.ReportMetric(float64(stats.CompactionCount), "compactions")
}