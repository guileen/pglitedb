package pebble

import (
	"testing"

	"github.com/guileen/pglitedb/types"
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

func TestBufferPoolFunctionality(t *testing.T) {
	rm := GetResourceManager()
	
	// Test acquiring buffer with specific size
	buf1 := rm.AcquireBuffer(100)
	if len(buf1) != 100 {
		t.Errorf("Expected buffer length 100, got %d", len(buf1))
	}
	
	// Test that buffer has sufficient capacity
	if cap(buf1) < 100 {
		t.Errorf("Expected buffer capacity >= 100, got %d", cap(buf1))
	}
	
	// Test releasing buffer
	rm.ReleaseBuffer(buf1)
	
	// Test acquiring buffer again (should come from pool)
	buf2 := rm.AcquireBuffer(50)
	if len(buf2) != 50 {
		t.Errorf("Expected buffer length 50, got %d", len(buf2))
	}
	
	// Test that we can reuse the buffer
	rm.ReleaseBuffer(buf2)
	
	// Test acquiring larger buffer than what's in pool
	buf3 := rm.AcquireBuffer(200)
	if len(buf3) != 200 {
		t.Errorf("Expected buffer length 200, got %d", len(buf3))
	}
	
	rm.ReleaseBuffer(buf3)
}

func TestEnhancedObjectPooling(t *testing.T) {
	rm := GetResourceManager()
	
	// Test record pool
	record := rm.AcquireRecord()
	if record == nil {
		t.Error("Failed to acquire record from pool")
	}
	
	// Add some data to the record
	record.Data["test"] = &types.Value{Data: "test", Type: types.ColumnTypeString}
	
	// Release the record back to pool
	rm.ReleaseRecord(record)
	
	// Acquire another record (should come from pool)
	record2 := rm.AcquireRecord()
	if record2 == nil {
		t.Error("Failed to acquire record from pool")
	}
	
	// The pooled record should be clean
	if len(record2.Data) != 0 {
		t.Error("Pooled record should be clean")
	}
	
	// Test iterator pool
	iter := rm.AcquireIterator()
	if iter == nil {
		t.Error("Failed to acquire iterator from pool")
	}
	
	// Release iterator back to pool
	rm.ReleaseIterator(iter)
	
	// Test batch pool
	batch := rm.AcquireBatch()
	if batch == nil {
		t.Error("Failed to acquire batch from pool")
	}
	
	// Release batch back to pool
	rm.ReleaseBatch(batch)
	
	// Test transaction pool
	txn := rm.AcquireTransaction()
	if txn == nil {
		t.Error("Failed to acquire transaction from pool")
	}
	
	// Release transaction back to pool
	rm.ReleaseTransaction(txn)
}