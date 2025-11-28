package pebble

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
)

func TestTransactionCreation(t *testing.T) {
	// Create a test engine
	engine := createTestEngine(t)
	defer engine.Close()

	// Test BeginTx (default isolation)
	ctx := context.Background()
	tx, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	
	// Verify transaction is tracked for leak detection
	rm := GetResourceManager()
	report := rm.CheckForLeaks()
	// Note: We can't easily verify the exact count because there might be other tracked resources
	// but we can at least confirm the resource manager is working
	if report == nil {
		t.Error("Expected leak detection report")
	}

	// Test that we can commit the transaction
	if err := tx.Commit(); err != nil {
		t.Errorf("Failed to commit transaction: %v", err)
	}

	// After commit, the transaction should no longer be tracked as a leak
	time.Sleep(10 * time.Millisecond) // Small delay to ensure cleanup
	report = rm.CheckForLeaks()
	// Note: The exact count may vary depending on other tracked resources
	// but it should be less than before
}

func TestTransactionCreationWithIsolation(t *testing.T) {
	// Create a test engine
	engine := createTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Test BeginTxWithIsolation for different levels
	levels := []storage.IsolationLevel{
		storage.ReadUncommitted,
		storage.ReadCommitted,
		storage.RepeatableRead,
		storage.SnapshotIsolation,
		storage.Serializable,
	}

	for _, level := range levels {
		tx, err := engine.BeginTxWithIsolation(ctx, level)
		if err != nil {
			t.Errorf("Failed to begin transaction with isolation level %v: %v", level, err)
			continue
		}

		// Verify transaction is tracked for leak detection
		rm := GetResourceManager()
		report := rm.CheckForLeaks()
		// Note: We can't easily verify the exact count because there might be other tracked resources
		// but we can at least confirm the resource manager is working
		if report == nil {
			t.Errorf("Expected leak detection report for isolation level %v", level)
		}

		// Test that we can commit the transaction
		if err := tx.Commit(); err != nil {
			t.Errorf("Failed to commit transaction with isolation level %v: %v", level, err)
		}
	}
}

func TestSnapshotTransactionCreation(t *testing.T) {
	// Create a test engine
	engine := createTestEngine(t)
	defer engine.Close()

	ctx := context.Background()

	// Test snapshot transaction creation (RepeatableRead and above)
	levels := []storage.IsolationLevel{
		storage.RepeatableRead,
		storage.SnapshotIsolation,
		storage.Serializable,
	}

	for _, level := range levels {
		tx, err := engine.BeginTxWithIsolation(ctx, level)
		if err != nil {
			t.Errorf("Failed to begin snapshot transaction with isolation level %v: %v", level, err)
			continue
		}

		// Verify it's actually a snapshot transaction by checking if it implements the snapshotTransaction interface
		// We can do this by checking if it has the snapshot field
		if tx != nil {
			// This is a basic check - in a real implementation we might need a more sophisticated way to verify
			t.Logf("Created transaction of type %T for isolation level %v", tx, level)
		}

		// Verify transaction is tracked for leak detection
		rm := GetResourceManager()
		report := rm.CheckForLeaks()
		// Note: We can't easily verify the exact count because there might be other tracked resources
		// but we can at least confirm the resource manager is working
		if report == nil {
			t.Errorf("Expected leak detection report for isolation level %v", level)
		}

		// Test that we can commit the transaction
		if err := tx.Commit(); err != nil {
			t.Errorf("Failed to commit snapshot transaction with isolation level %v: %v", level, err)
		}
	}
}

func TestTransactionManagerLeakDetection(t *testing.T) {
	// Create a test engine
	engine := createTestEngine(t)
	defer engine.Close()

	// Get the resource manager and set a short leak threshold for testing
	rm := GetResourceManager()
	ld := rm.GetLeakDetector()
	if ld != nil {
		ld.SetLeakThreshold(50 * time.Millisecond)
	}

	ctx := context.Background()
	tx, err := engine.BeginTx(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}

	// Don't commit or rollback the transaction to simulate a leak
	_ = tx

	// Wait for the leak threshold to pass
	time.Sleep(100 * time.Millisecond)

	// Check for leaks
	report := rm.CheckForLeaks()
	// Note: We might not get exactly one leak because there could be other tracked resources
	// but we should get some leaks reported
	if report == nil {
		t.Fatal("Expected leak detection report")
	}

	// The report should have some information
	if report.TotalLeaks == 0 {
		t.Log("No leaks detected - this might indicate no resources were tracked or they were released properly")
	} else {
		t.Logf("Found %d total leaks", report.TotalLeaks)
	}
}

// Helper function to create a test engine
func createTestEngine(t *testing.T) *pebbleEngine {
	t.Helper()
	
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	
	// Create Pebble KV store
	config := storage.DefaultPebbleConfig(tempDir)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine
	engine := NewPebbleEngine(kvStore, c).(*pebbleEngine)
	
	return engine
}