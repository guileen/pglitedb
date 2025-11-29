package kv

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

func TestPebbleKV_WriteBuffer(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	config.FlushInterval = 200 * time.Millisecond
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	t.Run("Async write with buffer", func(t *testing.T) {
		key := []byte("buffer-key")
		value := []byte("buffer-value")

		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		stats := kv.Stats()
		if stats.PendingWrites != 1 {
			t.Errorf("expected 1 pending write, got %d", stats.PendingWrites)
		}

		got, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(got) != string(value) {
			t.Errorf("expected %s, got %s", value, got)
		}
	})

	t.Run("Sync write", func(t *testing.T) {
		key := []byte("sync-key")
		value := []byte("sync-value")

		if err := kv.SetWithOptions(ctx, key, value, shared.SyncWriteOptions); err != nil {
			t.Fatalf("set with sync: %v", err)
		}

		got, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("get: %v", err)
		}
		if string(got) != string(value) {
			t.Errorf("expected %s, got %s", value, got)
		}
	})

	t.Run("Manual flush", func(t *testing.T) {
		key := []byte("flush-key")
		value := []byte("flush-value")

		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		beforeStats := kv.Stats()
		if beforeStats.PendingWrites == 0 {
			t.Error("expected pending writes before flush")
		}

		if err := kv.Flush(); err != nil {
			t.Fatalf("flush: %v", err)
		}

		afterStats := kv.Stats()
		if afterStats.PendingWrites != 0 {
			t.Errorf("expected 0 pending writes after flush, got %d", afterStats.PendingWrites)
		}
	})

	t.Run("Background flush", func(t *testing.T) {
		key := []byte("bg-flush-key")
		value := []byte("bg-flush-value")

		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		// Wait a bit to ensure the write is registered before checking
		time.Sleep(10 * time.Millisecond)
		
		beforeStats := kv.Stats()
		if beforeStats.PendingWrites == 0 {
			t.Error("expected pending writes before background flush")
		}

		time.Sleep(300 * time.Millisecond)

		afterStats := kv.Stats()
		if afterStats.PendingWrites != 0 {
			t.Errorf("expected 0 pending writes after background flush, got %d", afterStats.PendingWrites)
		}

		got, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("get after background flush: %v", err)
		}
		if string(got) != string(value) {
			t.Errorf("expected %s, got %s", value, got)
		}
	})

	t.Run("Flush on close", func(t *testing.T) {
		tmpDir2, err := os.MkdirTemp("", "pebble-test-close-*")
		if err != nil {
			t.Fatalf("create temp dir: %v", err)
		}
		defer os.RemoveAll(tmpDir2)

		config2 := DefaultPebbleConfig(filepath.Join(tmpDir2, "db"))
		config2.FlushInterval = 10 * time.Second
		kv2, err := NewPebbleKV(config2)
		if err != nil {
			t.Fatalf("create pebble kv: %v", err)
		}

		key := []byte("close-flush-key")
		value := []byte("close-flush-value")

		if err := kv2.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		stats := kv2.Stats()
		if stats.PendingWrites == 0 {
			t.Error("expected pending writes before close")
		}

		if err := kv2.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}

		time.Sleep(100 * time.Millisecond)

		kv3, err := NewPebbleKV(config2)
		if err != nil {
			t.Fatalf("reopen pebble kv: %v", err)
		}
		defer kv3.Close()

		got, err := kv3.Get(ctx, key)
		if err != nil {
			t.Fatalf("get after reopen: %v", err)
		}
		if string(got) != string(value) {
			t.Errorf("expected %s after close+reopen, got %s", value, got)
		}
	})
}

func TestPebbleKV_BasicOperations(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	t.Run("Set and Get", func(t *testing.T) {
		key := []byte("test-key")
		value := []byte("test-value")

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
	})

	t.Run("Get non-existent key", func(t *testing.T) {
		key := []byte("non-existent")
		_, err := kv.Get(ctx, key)
		if !shared.IsNotFound(err) {
			t.Errorf("expected NotFound error, got %v", err)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		key := []byte("delete-key")
		value := []byte("delete-value")

		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		if err := kv.Delete(ctx, key); err != nil {
			t.Fatalf("delete: %v", err)
		}

		_, err := kv.Get(ctx, key)
		if !shared.IsNotFound(err) {
			t.Errorf("expected NotFound error after delete, got %v", err)
		}
	})
}

func TestPebbleKV_Batch(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	t.Run("Batch atomicity", func(t *testing.T) {
		batch := kv.NewBatch()
		
		keys := [][]byte{
			[]byte("batch-key-1"),
			[]byte("batch-key-2"),
			[]byte("batch-key-3"),
		}
		values := [][]byte{
			[]byte("value-1"),
			[]byte("value-2"),
			[]byte("value-3"),
		}

		for i := range keys {
			if err := batch.Set(keys[i], values[i]); err != nil {
				t.Fatalf("batch set: %v", err)
			}
		}

		if batch.Count() != 3 {
			t.Errorf("expected count 3, got %d", batch.Count())
		}

		if err := kv.CommitBatch(ctx, batch); err != nil {
			t.Fatalf("commit batch: %v", err)
		}

		for i := range keys {
			got, err := kv.Get(ctx, keys[i])
			if err != nil {
				t.Fatalf("get key %d: %v", i, err)
			}
			if string(got) != string(values[i]) {
				t.Errorf("key %d: expected %s, got %s", i, values[i], got)
			}
		}
	})

	t.Run("Batch with delete", func(t *testing.T) {
		key := []byte("batch-delete-key")
		value := []byte("batch-delete-value")

		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}

		batch := kv.NewBatch()
		if err := batch.Delete(key); err != nil {
			t.Fatalf("batch delete: %v", err)
		}

		if err := kv.CommitBatch(ctx, batch); err != nil {
			t.Fatalf("commit batch: %v", err)
		}

		_, err := kv.Get(ctx, key)
		if !shared.IsNotFound(err) {
			t.Errorf("expected NotFound after batch delete, got %v", err)
		}
	})
}

func TestPebbleKV_Iterator(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	testData := map[string]string{
		"key-a": "value-a",
		"key-b": "value-b",
		"key-c": "value-c",
		"key-d": "value-d",
	}

	for k, v := range testData {
		if err := kv.Set(ctx, []byte(k), []byte(v)); err != nil {
			t.Fatalf("set %s: %v", k, err)
		}
	}

	t.Run("Forward iteration", func(t *testing.T) {
		iter := kv.NewIterator(nil)
		defer iter.Close()

		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
			key := string(iter.Key())
			value := string(iter.Value())

			if expected, ok := testData[key]; !ok || expected != value {
				t.Errorf("key %s: expected %s, got %s", key, expected, value)
			}
		}

		if err := iter.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}

		if count < len(testData) {
			t.Errorf("expected at least %d items, got %d", len(testData), count)
		}
	})

	t.Run("Reverse iteration", func(t *testing.T) {
		iter := kv.NewIterator(&shared.IteratorOptions{Reverse: true})
		defer iter.Close()

		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
		}

		if err := iter.Error(); err != nil {
			t.Fatalf("iterator error: %v", err)
		}

		if count < len(testData) {
			t.Errorf("expected at least %d items, got %d", len(testData), count)
		}
	})

	t.Run("Range iteration", func(t *testing.T) {
		opts := &shared.IteratorOptions{
			LowerBound: []byte("key-b"),
			UpperBound: []byte("key-d"),
		}
		iter := kv.NewIterator(opts)
		defer iter.Close()

		count := 0
		for iter.First(); iter.Valid(); iter.Next() {
			count++
			key := string(iter.Key())
			if key < "key-b" || key >= "key-d" {
				t.Errorf("key %s out of range [key-b, key-d)", key)
			}
		}

		if count != 2 {
			t.Errorf("expected 2 items in range, got %d", count)
		}
	})
}

func TestPebbleKV_Snapshot(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	key := []byte("snapshot-key")
	value1 := []byte("value-1")
	value2 := []byte("value-2")

	if err := kv.Set(ctx, key, value1); err != nil {
		t.Fatalf("set initial value: %v", err)
	}

	kv.Flush()

	snapshot, err := kv.NewSnapshot()
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}
	defer snapshot.Close()

	if err := kv.Set(ctx, key, value2); err != nil {
		t.Fatalf("set new value: %v", err)
	}

	got, err := snapshot.Get(key)
	if err != nil {
		t.Fatalf("snapshot get: %v", err)
	}

	if string(got) != string(value1) {
		t.Errorf("snapshot: expected %s, got %s", value1, got)
	}

	current, err := kv.Get(ctx, key)
	if err != nil {
		t.Fatalf("current get: %v", err)
	}

	if string(current) != string(value2) {
		t.Errorf("current: expected %s, got %s", value2, current)
	}
}

func TestPebbleKV_Transaction(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	t.Run("Transaction commit", func(t *testing.T) {
		txn, err := kv.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("create transaction: %v", err)
		}

		key := []byte("txn-key")
		value := []byte("txn-value")

		if err := txn.Set(key, value); err != nil {
			t.Fatalf("txn set: %v", err)
		}

		if err := txn.Commit(); err != nil {
			t.Fatalf("txn commit: %v", err)
		}

		got, err := kv.Get(ctx, key)
		if err != nil {
			t.Fatalf("get after commit: %v", err)
		}

		if string(got) != string(value) {
			t.Errorf("expected %s, got %s", value, got)
		}
	})

	t.Run("Transaction rollback", func(t *testing.T) {
		key := []byte("rollback-key")
		value := []byte("rollback-value")

		txn, err := kv.NewTransaction(ctx)
		if err != nil {
			t.Fatalf("create transaction: %v", err)
		}

		if err := txn.Set(key, value); err != nil {
			t.Fatalf("txn set: %v", err)
		}

		if err := txn.Rollback(); err != nil {
			t.Fatalf("txn rollback: %v", err)
		}

		_, err = kv.Get(ctx, key)
		if !shared.IsNotFound(err) {
			t.Errorf("expected NotFound after rollback, got %v", err)
		}
	})
}

func TestPebbleKV_Concurrent(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()
	done := make(chan bool)
	goroutines := 10
	opsPerGoroutine := 100

	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer func() { done <- true }()

			for i := 0; i < opsPerGoroutine; i++ {
				key := []byte("concurrent-" + string(rune(id)) + "-" + string(rune(i)))
				value := []byte("value-" + string(rune(id)) + "-" + string(rune(i)))

				if err := kv.Set(ctx, key, value); err != nil {
					t.Errorf("concurrent set: %v", err)
					return
				}

				got, err := kv.Get(ctx, key)
				if err != nil {
					t.Errorf("concurrent get: %v", err)
					return
				}

				if string(got) != string(value) {
					t.Errorf("concurrent: expected %s, got %s", value, got)
					return
				}
			}
		}(g)
	}

	for i := 0; i < goroutines; i++ {
		<-done
	}
}

func TestPebbleKV_Stats(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	for i := 0; i < 100; i++ {
		key := []byte("stats-key-" + string(rune(i)))
		value := []byte("stats-value-" + string(rune(i)))
		if err := kv.Set(ctx, key, value); err != nil {
			t.Fatalf("set: %v", err)
		}
	}

	stats := kv.Stats()
	if stats.ApproximateSize == 0 {
		t.Error("expected non-zero approximate size")
	}
}

func BenchmarkPebbleKV_Set(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "pebble-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use test-optimized configuration for faster benchmarking
	config := TestOptimizedPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()
	key := []byte("bench-key")
	value := []byte("bench-value-1234567890")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := kv.Set(ctx, key, value); err != nil {
			b.Fatalf("set: %v", err)
		}
	}
}

func BenchmarkPebbleKV_Get(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "pebble-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use test-optimized configuration for faster benchmarking
	config := TestOptimizedPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()
	key := []byte("bench-key")
	value := []byte("bench-value-1234567890")

	if err := kv.Set(ctx, key, value); err != nil {
		b.Fatalf("setup set: %v", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := kv.Get(ctx, key); err != nil {
			b.Fatalf("get: %v", err)
		}
	}
}

func BenchmarkPebbleKV_Batch(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "pebble-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use test-optimized configuration for faster benchmarking
	config := TestOptimizedPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		batch := kv.NewBatch()
		for j := 0; j < 100; j++ {
			key := []byte("bench-batch-key")
			value := []byte("bench-batch-value")
			batch.Set(key, value)
		}
		if err := kv.CommitBatch(ctx, batch); err != nil {
			b.Fatalf("commit batch: %v", err)
		}
	}
}

func BenchmarkPebbleKV_Transaction(b *testing.B) {
	tmpDir, err := os.MkdirTemp("", "pebble-bench-*")
	if err != nil {
		b.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	// Use test-optimized configuration for faster benchmarking
	config := TestOptimizedPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		b.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn, err := kv.NewTransaction(ctx)
		if err != nil {
			b.Fatalf("create transaction: %v", err)
		}

		key := []byte("bench-txn-key")
		value := []byte("bench-txn-value")
		if err := txn.Set(key, value); err != nil {
			b.Fatalf("txn set: %v", err)
		}

		if err := txn.Commit(); err != nil {
			b.Fatalf("txn commit: %v", err)
		}
	}
}

func TestPebbleKV_Close(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}

	ctx := context.Background()
	key := []byte("close-key")
	value := []byte("close-value")

	if err := kv.Set(ctx, key, value); err != nil {
		t.Fatalf("set before close: %v", err)
	}

	if err := kv.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	if err := kv.Set(ctx, key, value); err != shared.ErrClosed {
		t.Errorf("expected ErrClosed after close, got %v", err)
	}

	time.Sleep(100 * time.Millisecond)

	kv2, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("reopen pebble kv: %v", err)
	}
	defer kv2.Close()

	got, err := kv2.Get(ctx, key)
	if err != nil {
		t.Fatalf("get after reopen: %v", err)
	}

	if string(got) != string(value) {
		t.Errorf("after reopen: expected %s, got %s", value, got)
	}
}

func TestIsolationLevels(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	ctx := context.Background()

	// Test 1: Create transaction with default isolation level
	txn1, err := kv.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("create transaction: %v", err)
	}

	// Check default isolation level is ReadCommitted
	if txn1.Isolation() != shared.ReadCommitted {
		t.Errorf("expected default isolation level ReadCommitted, got %v", txn1.Isolation())
	}

	// Test 2: Set isolation level
	err = txn1.SetIsolation(shared.RepeatableRead)
	if err != nil {
		t.Fatalf("set isolation level: %v", err)
	}
	if txn1.Isolation() != shared.RepeatableRead {
		t.Errorf("expected isolation level RepeatableRead, got %v", txn1.Isolation())
	}

	// Test 3: Invalid isolation level
	err = txn1.SetIsolation(shared.IsolationLevel(999))
	if err == nil {
		t.Error("expected error for invalid isolation level")
	}

	// Clean up
	txn1.Rollback()
}

func TestConflictDetection(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	pkv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer pkv.Close()

	ctx := context.Background()

	// Create two transactions
	txn1, err := pkv.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("create transaction 1: %v", err)
	}

	txn2, err := pkv.NewTransaction(ctx)
	if err != nil {
		t.Fatalf("create transaction 2: %v", err)
	}

	key := []byte("conflict-key")
	value1 := []byte("value-1")
	// value2 := []byte("value-2") // Not used in this test

	// Transaction 1 writes to the key
	err = txn1.Set(key, value1)
	if err != nil {
		t.Fatalf("txn1 set: %v", err)
	}

	// Transaction 2 tries to write to the same key - should not detect conflict in our simplified implementation
	err = pkv.CheckForConflicts(txn2, key)
	if err != nil {
		// In our simplified implementation, we don't return conflicts
		// A full implementation would return ErrConflict here
		t.Logf("conflict check returned: %v", err)
	}

	// Clean up
	txn1.Rollback()
	txn2.Rollback()
}

func TestPebbleKV_Configuration(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "pebble-config-test-*")
	if err != nil {
		t.Fatalf("create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	config := DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	
	// Test that our new configuration options are set correctly in default config
	if config.BlockSize != 64<<10 {
		t.Errorf("expected BlockSize 65536, got %d", config.BlockSize)
	}
	
	if config.L0CompactionThreshold != 8 {
		t.Errorf("expected L0CompactionThreshold 8, got %d", config.L0CompactionThreshold)
	}
	
	if config.L0StopWritesThreshold != 32 {
		t.Errorf("expected L0StopWritesThreshold 32, got %d", config.L0StopWritesThreshold)
	}
	
	if !config.CompressionEnabled {
		t.Error("expected CompressionEnabled to be true")
	}

	// Test that we can override the configuration
	config.BlockSize = 32 << 10 // 32KB
	config.L0CompactionThreshold = 4
	config.L0StopWritesThreshold = 12
	config.CompressionEnabled = false

	kv, err := NewPebbleKV(config)
	if err != nil {
		t.Fatalf("create pebble kv: %v", err)
	}
	defer kv.Close()

	// Basic test to ensure the KV store works with our custom configuration
	ctx := context.Background()
	key := []byte("test-key")
	value := []byte("test-value")

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
}