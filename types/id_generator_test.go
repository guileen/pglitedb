package types

import (
	"sync"
	"testing"
	"time"
)

func TestSnowflakeIDGenerator(t *testing.T) {
	gen := NewSnowflakeIDGenerator(0)

	t.Run("UniqueIDs", func(t *testing.T) {
		seen := make(map[int64]bool)
		for i := 0; i < 10000; i++ {
			id, err := gen.Next()
			if err != nil {
				t.Fatalf("failed to generate id: %v", err)
			}
			if seen[id] {
				t.Fatalf("duplicate id: %d", id)
			}
			seen[id] = true
		}
	})

	t.Run("Concurrent", func(t *testing.T) {
		gen := NewSnowflakeIDGenerator(1)
		ids := make(chan int64, 1000)
		var wg sync.WaitGroup

		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 100; j++ {
					id, err := gen.Next()
					if err != nil {
						t.Errorf("failed to generate id: %v", err)
						return
					}
					ids <- id
				}
			}()
		}

		wg.Wait()
		close(ids)

		seen := make(map[int64]bool)
		for id := range ids {
			if seen[id] {
				t.Fatalf("duplicate id in concurrent test: %d", id)
			}
			seen[id] = true
		}
	})

	t.Run("MonotonicIncreasing", func(t *testing.T) {
		gen := NewSnowflakeIDGenerator(2)
		var lastID int64
		for i := 0; i < 1000; i++ {
			id, err := gen.Next()
			if err != nil {
				t.Fatalf("failed to generate id: %v", err)
			}
			if id <= lastID {
				t.Fatalf("id not monotonic: %d <= %d", id, lastID)
			}
			lastID = id
		}
	})

	t.Run("Performance", func(t *testing.T) {
		gen := NewSnowflakeIDGenerator(3)
		start := time.Now()
		count := 100000
		for i := 0; i < count; i++ {
			_, err := gen.Next()
			if err != nil {
				t.Fatalf("failed to generate id: %v", err)
			}
		}
		duration := time.Since(start)
		idsPerSec := float64(count) / duration.Seconds()
		t.Logf("Generated %d IDs in %v (%.0f IDs/sec)", count, duration, idsPerSec)
		if idsPerSec < 100000 {
			t.Errorf("performance too low: %.0f IDs/sec", idsPerSec)
		}
	})
}

func TestSnowflakeIDGeneratorMachineID(t *testing.T) {
	t.Run("ValidMachineID", func(t *testing.T) {
		for _, machineID := range []int64{0, 1, 512, 1023} {
			gen := NewSnowflakeIDGenerator(machineID)
			_, err := gen.Next()
			if err != nil {
				t.Errorf("failed with valid machineID %d: %v", machineID, err)
			}
		}
	})

	t.Run("InvalidMachineID", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for negative machineID")
			}
		}()
		NewSnowflakeIDGenerator(-1)
	})

	t.Run("InvalidMachineIDTooLarge", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic for machineID > 1023")
			}
		}()
		NewSnowflakeIDGenerator(1024)
	})
}
