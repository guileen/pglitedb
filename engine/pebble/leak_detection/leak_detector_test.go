package leak_detection

import (
	"testing"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
)

func TestLeakDetector(t *testing.T) {
	// Create a new leak detector
	ld := NewLeakDetector()
	
	// Set a short leak threshold for testing
	ld.SetLeakThreshold(100 * time.Millisecond)
	
	// Create a test object to track
	testObj := &struct{ name string }{name: "test"}
	
	// Track the object
	tracked := ld.TrackIterator(testObj, "test stack trace")
	
	// Check that the object is tracked
	if tracked == nil {
		t.Fatal("Expected tracked resource, got nil")
	}
	
	// Check that the resource is not leaked initially
	if tracked.IsLeaked() {
		t.Fatal("Expected resource not to be leaked initially")
	}
	
	// Wait for the leak threshold to pass
	time.Sleep(150 * time.Millisecond)
	
	// Check that the resource is now leaked
	if !tracked.IsLeaked() {
		t.Fatal("Expected resource to be leaked after threshold")
	}
	
	// Mark the resource as released
	tracked.MarkReleased()
	
	// Check that the resource is no longer leaked
	if tracked.IsLeaked() {
		t.Fatal("Expected resource not to be leaked after being released")
	}
}

func TestLeakReport(t *testing.T) {
	// Create a new leak detector
	ld := NewLeakDetector()
	
	// Set a short leak threshold for testing
	ld.SetLeakThreshold(10 * time.Millisecond)
	
	// Create test objects to track
	testObj1 := &struct{ name string }{name: "test1"}
	testObj2 := &struct{ name string }{name: "test2"}
	
	// Track the objects
	ld.TrackIterator(testObj1, "stack trace 1")
	tracked2 := ld.TrackTransaction(testObj2, "stack trace 2")
	
	// Wait for the leak threshold to pass
	time.Sleep(20 * time.Millisecond)
	
	// Check for leaks
	report := ld.CheckForLeaks()
	
	// Should have 2 leaks
	if report.TotalLeaks != 2 {
		t.Fatalf("Expected 2 leaks, got %d", report.TotalLeaks)
	}
	
	// Mark one resource as released
	tracked2.MarkReleased()
	
	// Check for leaks again
	report = ld.CheckForLeaks()
	
	// Should have 1 leak now
	if report.TotalLeaks != 1 {
		t.Fatalf("Expected 1 leak, got %d", report.TotalLeaks)
	}
}

func TestResourceTypeMetrics(t *testing.T) {
	// Create a new leak detector
	ld := NewLeakDetector()
	
	// Create test objects to track
	testObj1 := &struct{ name string }{name: "test1"}
	testObj2 := &struct{ name string }{name: "test2"}
	testObj3 := &struct{ name string }{name: "test3"}
	
	// Track different types of resources
	ld.TrackIterator(testObj1, "stack trace 1")
	ld.TrackTransaction(testObj2, "stack trace 2")
	ld.TrackConnection(testObj3, "stack trace 3")
	
	// Get metrics
	metrics := ld.GetMetrics()
	
	// Check that all resources are tracked
	if metrics.TotalTracked != 3 {
		t.Fatalf("Expected 3 tracked resources, got %d", metrics.TotalTracked)
	}
	
	// Check resource type metrics
	if metrics.ResourceTypes[engineTypes.ResourceTypeIterator].Tracked != 1 {
		t.Fatalf("Expected 1 iterator, got %d", metrics.ResourceTypes[engineTypes.ResourceTypeIterator].Tracked)
	}
	
	if metrics.ResourceTypes[engineTypes.ResourceTypeTransaction].Tracked != 1 {
		t.Fatalf("Expected 1 transaction, got %d", metrics.ResourceTypes[engineTypes.ResourceTypeTransaction].Tracked)
	}
	
	if metrics.ResourceTypes[engineTypes.ResourceTypeConnection].Tracked != 1 {
		t.Fatalf("Expected 1 connection, got %d", metrics.ResourceTypes[engineTypes.ResourceTypeConnection].Tracked)
	}
}