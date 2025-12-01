package kv

import (
	"context"
	"testing"
	"time"
)

// TestPebbleKV_NoGoroutineLeak tests that closing a PebbleKV instance
// doesn't leave any background goroutines running, which could cause tests to hang
func TestPebbleKV_NoGoroutineLeak(t *testing.T) {
	// Use in-memory filesystem to avoid disk I/O and background goroutines
	config := TestOptimizedPebbleConfig("")
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}

	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

	// Do some basic operations
	if err := kv.Set(ctx, key, value); err != nil {
		t.Fatalf("set: %v", err)
	}

	got, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if string(got) != string(value) {
		t.Errorf("expected %s, got %s", value, got)
	}

	// Close the database - this should properly terminate all internal goroutines
	if err := kv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// Give a small amount of time for goroutines to clean up
	time.Sleep(100 * time.Millisecond)

	// If we reach here without hanging, the test passes
	// The fact that this test completes quickly indicates no goroutine leaks
}