package transaction

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// Test all possible state transitions for transactions
func TestTransactionStateTransitions(t *testing.T) {
	// Create temporary directory for test
	tempDir := t.TempDir()

	// Create KV store
	kvPath := tempDir + "/kv"
	config := storage.DefaultPebbleConfig(kvPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create transaction manager
	manager, err := NewManager(&Config{
		LogPath: tempDir,
		KV:      kvStore,
	})
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// Test 1: Active -> Committed
	t.Run("ActiveToCommitted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.State != TxnActive {
			t.Errorf("Expected state TxnActive, got %v", txn.State)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		if txn.State != TxnCommitted {
			t.Errorf("Expected state TxnCommitted, got %v", txn.State)
		}
	})

	// Test 2: Active -> RolledBack
	t.Run("ActiveToRolledBack", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.State != TxnActive {
			t.Errorf("Expected state TxnActive, got %v", txn.State)
		}

		err = manager.Rollback(txn)
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		if txn.State != TxnRolledBack {
			t.Errorf("Expected state TxnRolledBack, got %v", txn.State)
		}
	})

	// Test 3: Active -> Aborted
	t.Run("ActiveToAborted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		if txn.State != TxnActive {
			t.Errorf("Expected state TxnActive, got %v", txn.State)
		}

		err = manager.Abort(txn)
		if err != nil {
			t.Fatalf("Failed to abort transaction: %v", err)
		}

		if txn.State != TxnAborted {
			t.Errorf("Expected state TxnAborted, got %v", txn.State)
		}
	})

	// Test 4: Invalid transitions from Committed
	t.Run("InvalidTransitionsFromCommitted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Commit the transaction first
		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}

		// Try to commit again (should fail)
		err = manager.Commit(txn)
		if err == nil {
			t.Error("Should not be able to commit an already committed transaction")
		}

		// Try to rollback (should fail)
		err = manager.Rollback(txn)
		if err == nil {
			t.Error("Should not be able to rollback an already committed transaction")
		}

		// Try to abort (should fail)
		err = manager.Abort(txn)
		if err == nil {
			t.Error("Should not be able to abort an already committed transaction")
		}
	})

	// Test 5: Invalid transitions from RolledBack
	t.Run("InvalidTransitionsFromRolledBack", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Rollback the transaction first
		err = manager.Rollback(txn)
		if err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}

		// Try to commit (should fail)
		err = manager.Commit(txn)
		if err == nil {
			t.Error("Should not be able to commit an already rolled back transaction")
		}

		// Try to rollback again (should fail)
		err = manager.Rollback(txn)
		if err == nil {
			t.Error("Should not be able to rollback an already rolled back transaction")
		}

		// Try to abort (should fail)
		err = manager.Abort(txn)
		if err == nil {
			t.Error("Should not be able to abort an already rolled back transaction")
		}
	})

	// Test 6: Invalid transitions from Aborted
	t.Run("InvalidTransitionsFromAborted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction: %v", err)
		}

		// Abort the transaction first
		err = manager.Abort(txn)
		if err != nil {
			t.Fatalf("Failed to abort transaction: %v", err)
		}

		// Try to commit (should fail)
		err = manager.Commit(txn)
		if err == nil {
			t.Error("Should not be able to commit an already aborted transaction")
		}

		// Try to rollback (should fail)
		err = manager.Rollback(txn)
		if err == nil {
			t.Error("Should not be able to rollback an already aborted transaction")
		}

		// Try to abort again (should fail)
		err = manager.Abort(txn)
		if err == nil {
			t.Error("Should not be able to abort an already aborted transaction")
		}
	})
}

// Test transaction context cancellation
func TestTransactionContextCancellation(t *testing.T) {
	tempDir := t.TempDir()

	// Create KV store
	kvPath := tempDir + "/kv"
	config := storage.DefaultPebbleConfig(kvPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create transaction manager
	manager, err := NewManager(&Config{
		LogPath: tempDir,
		KV:      kvStore,
	})
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer manager.Close()

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	txn, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Wait for context to be cancelled
	time.Sleep(150 * time.Millisecond)

	// The transaction context should be cancelled, but the transaction state should remain
	// unless explicitly rolled back or committed
	if txn.State != TxnActive {
		t.Errorf("Expected transaction to remain active after context cancellation, got %v", txn.State)
	}
}

// Test transaction with different isolation levels
func TestTransactionIsolationLevels(t *testing.T) {
	tempDir := t.TempDir()

	// Create KV store
	kvPath := tempDir + "/kv"
	config := storage.DefaultPebbleConfig(kvPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create transaction manager
	manager, err := NewManager(&Config{
		LogPath: tempDir,
		KV:      kvStore,
	})
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()

	// Test each isolation level by name rather than using String() method
	t.Run("ReadUncommitted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadUncommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction with ReadUncommitted: %v", err)
		}

		if txn.Isolation != ReadUncommitted {
			t.Errorf("Expected isolation level ReadUncommitted, got %v", txn.Isolation)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})

	t.Run("ReadCommitted", func(t *testing.T) {
		txn, err := manager.Begin(ctx, ReadCommitted)
		if err != nil {
			t.Fatalf("Failed to begin transaction with ReadCommitted: %v", err)
		}

		if txn.Isolation != ReadCommitted {
			t.Errorf("Expected isolation level ReadCommitted, got %v", txn.Isolation)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})

	t.Run("RepeatableRead", func(t *testing.T) {
		txn, err := manager.Begin(ctx, RepeatableRead)
		if err != nil {
			t.Fatalf("Failed to begin transaction with RepeatableRead: %v", err)
		}

		if txn.Isolation != RepeatableRead {
			t.Errorf("Expected isolation level RepeatableRead, got %v", txn.Isolation)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})

	t.Run("SnapshotIsolation", func(t *testing.T) {
		txn, err := manager.Begin(ctx, SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin transaction with SnapshotIsolation: %v", err)
		}

		if txn.Isolation != SnapshotIsolation {
			t.Errorf("Expected isolation level SnapshotIsolation, got %v", txn.Isolation)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})

	t.Run("Serializable", func(t *testing.T) {
		txn, err := manager.Begin(ctx, Serializable)
		if err != nil {
			t.Fatalf("Failed to begin transaction with Serializable: %v", err)
		}

		if txn.Isolation != Serializable {
			t.Errorf("Expected isolation level Serializable, got %v", txn.Isolation)
		}

		err = manager.Commit(txn)
		if err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})
}

// Test transaction statistics and metadata
func TestTransactionMetadata(t *testing.T) {
	tempDir := t.TempDir()

	// Create KV store
	kvPath := tempDir + "/kv"
	config := storage.DefaultPebbleConfig(kvPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create transaction manager
	manager, err := NewManager(&Config{
		LogPath: tempDir,
		KV:      kvStore,
	})
	if err != nil {
		t.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer manager.Close()

	ctx := context.Background()

	txn, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Check that start time is set
	if txn.StartTime.IsZero() {
		t.Error("Transaction start time should be set")
	}

	// Check that snapshot timestamp is set
	if txn.SnapshotTS == 0 {
		t.Error("Transaction snapshot timestamp should be set")
	}

	// Perform some operations
	key := []byte("test_key")
	value := []byte("test_value")

	err = manager.Put(txn, key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}

	// Check that the key is in the write set
	if !txn.WasWritten("test_key") {
		t.Error("Key should be in write set after Put operation")
	}

	// Check read set functionality
	txn.AddToReadSet("read_key", txn.SnapshotTS)
	ts, exists := txn.GetReadTimestamp("read_key")
	if !exists {
		t.Error("Read key should exist in read set")
	}
	if ts != txn.SnapshotTS {
		t.Errorf("Expected timestamp %d, got %d", txn.SnapshotTS, ts)
	}

	// Commit and check commit time
	beforeCommit := time.Now()
	err = manager.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	afterCommit := time.Now()

	if txn.CommitTime.Before(beforeCommit) || txn.CommitTime.After(afterCommit.Add(time.Second)) {
		t.Error("Commit time should be set correctly")
	}

	if txn.CommitTS == 0 {
		t.Error("Commit timestamp should be set")
	}
}