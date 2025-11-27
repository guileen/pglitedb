package transaction

import (
	"context"
	"testing"
)

func TestTxnManager_Begin(t *testing.T) {
	tm := NewTxnManager()
	
	ctx := context.Background()
	txn, err := tm.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	if txn == nil {
		t.Fatal("Transaction should not be nil")
	}
	
	if txn.State != TxnActive {
		t.Errorf("Expected state TxnActive, got %v", txn.State)
	}
	
	if txn.Isolation != ReadCommitted {
		t.Errorf("Expected isolation ReadCommitted, got %v", txn.Isolation)
	}
	
	if !txn.IsActive() {
		t.Error("Transaction should be active")
	}
}

func TestTxnManager_Commit(t *testing.T) {
	tm := NewTxnManager()
	
	ctx := context.Background()
	txn, err := tm.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	err = tm.Commit(txn)
	if err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}
	
	if txn.State != TxnCommitted {
		t.Errorf("Expected state TxnCommitted, got %v", txn.State)
	}
	
	if !txn.IsCommitted() {
		t.Error("Transaction should be committed")
	}
	
	if txn.IsRolledBack() {
		t.Error("Transaction should not be rolled back")
	}
}

func TestTxnManager_Rollback(t *testing.T) {
	tm := NewTxnManager()
	
	ctx := context.Background()
	txn, err := tm.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	err = tm.Rollback(txn)
	if err != nil {
		t.Fatalf("Failed to rollback transaction: %v", err)
	}
	
	if txn.State != TxnRolledBack {
		t.Errorf("Expected state TxnRolledBack, got %v", txn.State)
	}
	
	if !txn.IsRolledBack() {
		t.Error("Transaction should be rolled back")
	}
	
	if txn.IsCommitted() {
		t.Error("Transaction should not be committed")
	}
}

func TestTxnContext_Savepoints(t *testing.T) {
	tm := NewTxnManager()
	
	ctx := context.Background()
	txn, err := tm.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Create savepoint
	err = txn.CreateSavepoint("sp1")
	if err != nil {
		t.Fatalf("Failed to create savepoint: %v", err)
	}
	
	// Rollback to savepoint
	err = txn.RollbackToSavepoint("sp1")
	if err != nil {
		t.Fatalf("Failed to rollback to savepoint: %v", err)
	}
	
	// Release savepoint
	err = txn.ReleaseSavepoint("sp1")
	if err != nil {
		t.Fatalf("Failed to release savepoint: %v", err)
	}
}

func TestTxnContext_ReadWriteSets(t *testing.T) {
	tm := NewTxnManager()
	
	ctx := context.Background()
	txn, err := tm.Begin(ctx, ReadCommitted)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Test read set
	txn.AddToReadSet("key1", 100)
	ts, exists := txn.GetReadTimestamp("key1")
	if !exists {
		t.Error("Key should exist in read set")
	}
	if ts != 100 {
		t.Errorf("Expected timestamp 100, got %d", ts)
	}
	
	// Test write set
	txn.AddToWriteSet("key2")
	if !txn.WasWritten("key2") {
		t.Error("Key should be marked as written")
	}
}