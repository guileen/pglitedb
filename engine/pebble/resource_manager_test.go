package pebble

import (
	"testing"
)

func TestResourceManagerWithMetrics(t *testing.T) {
	rm := NewResourceManager()
	
	// Test iterator pooling with metrics
	iter := rm.AcquireIterator()
	if iter == nil {
		t.Fatal("Failed to acquire iterator")
	}
	
	rm.ReleaseIterator(iter)
	
	// Test batch pooling with metrics
	batch := rm.AcquireBatch()
	if batch == nil {
		t.Fatal("Failed to acquire batch")
	}
	
	rm.ReleaseBatch(batch)
	
	// Test transaction pooling with metrics
	txn := rm.AcquireTransaction()
	if txn == nil {
		t.Fatal("Failed to acquire transaction")
	}
	
	rm.ReleaseTransaction(txn)
	
	// Get metrics
	metrics := rm.GetResourceMetrics()
	
	// Verify metrics were recorded
	if metrics.IteratorAcquired == 0 {
		t.Error("Expected iterator acquired metric to be recorded")
	}
	
	if metrics.BatchAcquired == 0 {
		t.Error("Expected batch acquired metric to be recorded")
	}
	
	if metrics.TxnAcquired == 0 {
		t.Error("Expected transaction acquired metric to be recorded")
	}
	
	// Test metrics reset
	rm.ResetResourceMetrics()
	resetMetrics := rm.GetResourceMetrics()
	
	// Most metrics should be zero after reset
	if resetMetrics.IteratorAcquired != 0 {
		t.Errorf("Expected iterator acquired to be 0 after reset, got %d", resetMetrics.IteratorAcquired)
	}
	
	if resetMetrics.BatchAcquired != 0 {
		t.Errorf("Expected batch acquired to be 0 after reset, got %d", resetMetrics.BatchAcquired)
	}
	
	if resetMetrics.TxnAcquired != 0 {
		t.Errorf("Expected transaction acquired to be 0 after reset, got %d", resetMetrics.TxnAcquired)
	}
}