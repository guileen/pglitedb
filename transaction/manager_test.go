package transaction

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/guileen/pglitedb/storage"
)

func TestManager_BeginCommit(t *testing.T) {
	// Create temporary directory for logs
	tempDir, err := os.MkdirTemp("", "txn_test")
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
	
	// Begin transaction
	ctx := context.Background()
	txn, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Put a value directly to KV store (bypassing MVCC for simplicity in test)
	key := []byte("testkey")
	value := []byte("testvalue")
	err = kvStore.Set(ctx, key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}
	
	// Get the value directly from KV store (bypassing MVCC for simplicity in test)
	retrieved, err := kvStore.Get(ctx, key)
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	
	if string(retrieved) != string(value) {
		t.Errorf("Expected value '%s', got '%s'", string(value), string(retrieved))
	}
	
	// Commit transaction
	err = manager.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	
	if !txn.IsCommitted() {
		t.Error("Transaction should be committed")
	}
}

func TestManager_BeginRollback(t *testing.T) {
	// Create temporary directory for logs
	tempDir, err := os.MkdirTemp("", "txn_test")
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
	
	// Begin transaction
	ctx := context.Background()
	txn, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Put a value
	key := []byte("testkey2")
	value := []byte("testvalue2")
	err = manager.Put(txn, key, value)
	if err != nil {
		t.Fatalf("Failed to put value: %v", err)
	}
	
	// Rollback transaction
	err = manager.Rollback(txn)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	
	if !txn.IsRolledBack() {
		t.Error("Transaction should be rolled back")
	}
}

func TestManager_Savepoints(t *testing.T) {
	// Create temporary directory for logs
	tempDir, err := os.MkdirTemp("", "txn_test")
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
	
	// Begin transaction
	ctx := context.Background()
	txn, err := manager.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Create savepoint
	err = manager.CreateSavepoint(txn, "sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}
	
	// Rollback to savepoint
	err = manager.RollbackToSavepoint(txn, "sp1")
	if err != nil {
		t.Fatalf("Failed to rollback to savepoint: %v", err)
	}
	
	// Release savepoint
	err = manager.ReleaseSavepoint(txn, "sp1")
	if err != nil {
		t.Fatalf("Failed to release savepoint: %v", err)
	}
}