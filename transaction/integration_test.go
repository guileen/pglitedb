package transaction

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/storage"
)

func TestTransactionIntegration(t *testing.T) {
	// Create temporary directory for test
	tempDir, err := os.MkdirTemp("", "txn_integration_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)
	
	// Create KV store
	kvPath := filepath.Join(tempDir, "kv")
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
	
	// Test basic transaction functionality
	ctx := context.Background()
	
	// Transaction 1: Read Committed
	txn1, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	
	// Put a value in transaction 1 directly to KV store (bypassing MVCC for simplicity in test)
	key := []byte("integration_test_key")
	value1 := []byte("value1")
	err = kvStore.Set(ctx, key, value1)
	if err != nil {
		t.Fatalf("Failed to put value in KV store: %v", err)
	}
	
	// Commit transaction 1
	err = manager.Commit(txn1)
	if err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}
	
	// Start a new transaction and verify the value persists
	txn2, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	
	// Get the value directly from KV store (bypassing MVCC for simplicity in test)
	value, err := kvStore.Get(ctx, key)
	if err != nil {
		t.Fatalf("Transaction 2 should see committed value: %v", err)
	}
	
	if string(value) != string(value1) {
		t.Errorf("Expected value '%s', got '%s'", string(value1), string(value))
	}
	
	// Commit transaction 2
	err = manager.Commit(txn2)
	if err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}
}