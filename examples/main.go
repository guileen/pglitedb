package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/transaction"
)

func main() {
	// Create temporary directory for demo
	tempDir, err := os.MkdirTemp("", "txn_demo")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create KV store
	kvPath := filepath.Join(tempDir, "kv")
	config := storage.DefaultPebbleConfig(kvPath)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		log.Fatalf("Failed to create KV store: %v", err)
	}
	defer kvStore.Close()

	// Create transaction manager
	manager, err := transaction.NewManager(&transaction.Config{
		LogPath: tempDir,
		KV:      kvStore,
	})
	if err != nil {
		log.Fatalf("Failed to create transaction manager: %v", err)
	}
	defer manager.Close()

	// Demonstrate transaction usage
	ctx := context.Background()

	// Begin a transaction
	txn, err := manager.Begin(ctx, transaction.ReadCommitted)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	fmt.Println("Started transaction:", txn.ID)

	// Put some values
	key1 := []byte("user:1:name")
	value1 := []byte("Alice")

	err = manager.Put(txn, key1, value1)
	if err != nil {
		log.Fatalf("Failed to put value: %v", err)
	}

	fmt.Printf("Put key=%s, value=%s\n", string(key1), string(value1))

	// Create a savepoint
	err = manager.CreateSavepoint(txn, "before_update")
	if err != nil {
		log.Fatalf("Failed to create savepoint: %v", err)
	}

	fmt.Println("Created savepoint: before_update")

	// Update the value
	value1Updated := []byte("Alice Smith")
	err = manager.Put(txn, key1, value1Updated)
	if err != nil {
		log.Fatalf("Failed to update value: %v", err)
	}

	fmt.Printf("Updated key=%s, value=%s\n", string(key1), string(value1Updated))

	// Get the value
	retrieved, err := manager.Get(txn, key1)
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}

	fmt.Printf("Retrieved key=%s, value=%s\n", string(key1), string(retrieved))

	// Commit the transaction
	err = manager.Commit(txn)
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Println("Committed transaction:", txn.ID)

	// Start a new transaction to verify persistence
	txn2, err := manager.Begin(ctx, transaction.ReadCommitted)
	if err != nil {
		log.Fatalf("Failed to begin transaction: %v", err)
	}

	// Get the value in the new transaction
	retrieved2, err := manager.Get(txn2, key1)
	if err != nil {
		log.Fatalf("Failed to get value: %v", err)
	}

	fmt.Printf("Retrieved in new transaction key=%s, value=%s\n", string(key1), string(retrieved2))

	// Commit the second transaction
	err = manager.Commit(txn2)
	if err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Println("Demo completed successfully!")

	// Print statistics
	stats := manager.GetStats()
	fmt.Printf("Transaction stats: %+v\n", stats)
}