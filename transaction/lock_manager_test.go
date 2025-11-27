package transaction

import (
	"context"
	"testing"
)

func TestLockManager_LockUnlock(t *testing.T) {
	lm := NewLockManager()
	tm := NewTxnManager()
	
	// Create transactions
	txn1, _ := tm.Begin(context.Background(), ReadCommitted)
	txn2, _ := tm.Begin(context.Background(), ReadCommitted)
	
	lm.RegisterTransaction(txn1)
	lm.RegisterTransaction(txn2)
	
	// Acquire shared lock
	err := lm.Lock(txn1.ID, "resource1", LockShared, LockModeNonBlock)
	if err != nil {
		t.Fatalf("Failed to acquire shared lock: %v", err)
	}
	
	// Try to acquire another shared lock (should succeed)
	err = lm.Lock(txn2.ID, "resource1", LockShared, LockModeNonBlock)
	if err != nil {
		t.Fatalf("Should be able to acquire shared lock when another shared lock exists: %v", err)
	}
	
	// Try to acquire exclusive lock (should fail)
	err = lm.Lock(txn1.ID, "resource1", LockExclusive, LockModeNonBlock)
	if err == nil {
		t.Fatal("Should not be able to acquire exclusive lock when shared locks exist")
	}
	
	// Release locks - each transaction should release its own lock
	err = lm.Unlock(txn1.ID, "resource1")
	if err != nil {
		t.Fatalf("Failed to release lock for txn1: %v", err)
	}
	
	err = lm.Unlock(txn2.ID, "resource1")
	if err != nil {
		t.Fatalf("Failed to release lock for txn2: %v", err)
	}
}

func TestLockManager_RegisterUnregister(t *testing.T) {
	lm := NewLockManager()
	tm := NewTxnManager()
	
	txn, _ := tm.Begin(context.Background(), ReadCommitted)
	
	// Register transaction
	lm.RegisterTransaction(txn)
	
	// Unregister transaction
	lm.UnregisterTransaction(txn.ID)
}

func TestLockManager_GetLockInfo(t *testing.T) {
	lm := NewLockManager()
	tm := NewTxnManager()
	
	txn, _ := tm.Begin(context.Background(), ReadCommitted)
	lm.RegisterTransaction(txn)
	
	// Acquire lock
	lm.Lock(txn.ID, "resource1", LockShared, LockModeNonBlock)
	
	// Get lock info
	lock, exists := lm.GetLockInfo("resource1")
	if !exists {
		t.Fatal("Lock should exist")
	}
	
	if lock.ResourceID != "resource1" {
		t.Errorf("Expected resource ID 'resource1', got '%s'", lock.ResourceID)
	}
	
	if lock.LockType != LockShared {
		t.Errorf("Expected lock type LockShared, got %v", lock.LockType)
	}
}