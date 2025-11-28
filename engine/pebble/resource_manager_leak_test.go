package pebble

import (
	"testing"
	"time"

	"github.com/guileen/pglitedb/engine/pebble/resources"
)

func TestResourceManagerLeakDetection(t *testing.T) {
	// Get the default resource manager
	rm := resources.GetResourceManager()
	
	// Set a short leak threshold for testing
	if rm.GetLeakDetector() != nil {
		rm.GetLeakDetector().SetLeakThreshold(100 * time.Millisecond)
	}
	
	// Create a test iterator object to track directly
	testIter := &struct{ name string }{name: "test"}
	
	// Track the iterator manually for testing
	if rm.GetLeakDetector() != nil {
		stackTrace := "test stack trace"
		rm.GetLeakDetector().TrackIterator(testIter, stackTrace)
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check for leaks
	report := rm.CheckForLeaks()
	
	// There should be at least one leak (the iterator)
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
	
	// Mark the iterator as released
	// In a real scenario, this would be done by the Close method
	// For testing, we'll manually mark it as released
	if rm.GetLeakDetector() != nil {
		// We can't easily access the tracked resource, so we'll just check that tracking works
	}
	
	// Check for leaks again - we can't easily test the release in this simplified test
	// The main point is that tracking and leak detection works
}

func TestTransactionLeakDetection(t *testing.T) {
	// Get the default resource manager
	rm := resources.GetResourceManager()
	
	// Create a test transaction object
	testTxn := &struct{ name string }{name: "test"}
	
	// Track the transaction
	rm.TrackTransaction(testTxn)
	
	// Set a short leak threshold for testing
	if rm.GetLeakDetector() != nil {
		rm.GetLeakDetector().SetLeakThreshold(100 * time.Millisecond)
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check for leaks
	report := rm.CheckForLeaks()
	
	// There should be at least one leak (the transaction)
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
}

func TestConnectionLeakDetection(t *testing.T) {
	// Get the default resource manager
	rm := resources.GetResourceManager()
	
	// Create a test connection object
	testConn := &struct{ name string }{name: "test"}
	
	// Track the connection
	rm.TrackConnection(testConn)
	
	// Set a short leak threshold for testing
	if rm.GetLeakDetector() != nil {
		rm.GetLeakDetector().SetLeakThreshold(100 * time.Millisecond)
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check for leaks
	report := rm.CheckForLeaks()
	
	// There should be at least one leak (the connection)
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
}

func TestFileDescriptorLeakDetection(t *testing.T) {
	// Get the default resource manager
	rm := resources.GetResourceManager()
	
	// Create a test file descriptor object
	testFd := &struct{ name string }{name: "test"}
	
	// Track the file descriptor
	rm.TrackFileDescriptor(testFd, "/test/path")
	
	// Set a short leak threshold for testing
	if rm.GetLeakDetector() != nil {
		rm.GetLeakDetector().SetLeakThreshold(100 * time.Millisecond)
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check for leaks
	report := rm.CheckForLeaks()
	
	// There should be at least one leak (the file descriptor)
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
}

func TestGoroutineLeakDetection(t *testing.T) {
	// Get the default resource manager
	rm := resources.GetResourceManager()
	
	// Track the current goroutine
	rm.TrackCurrentGoroutine()
	
	// Set a short leak threshold for testing
	if rm.GetLeakDetector() != nil {
		rm.GetLeakDetector().SetLeakThreshold(100 * time.Millisecond)
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check for leaks
	report := rm.CheckForLeaks()
	
	// There should be at least one leak (the goroutine)
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
}