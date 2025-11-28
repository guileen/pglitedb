package leak_detection

import (
	"context"
	"testing"
	"time"
)

func TestGoroutineTracking(t *testing.T) {
	ld := NewLeakDetector()
	ld.SetLeakThreshold(50 * time.Millisecond)
	
	goroutineID := GetCurrentGoroutineID()
	_ = ld.TrackGoroutine(goroutineID, "test goroutine")
	
	time.Sleep(100 * time.Millisecond)
	
	report := ld.CheckForLeaks()
	if report.TotalLeaks == 0 {
		t.Fatal("Expected goroutine leak")
	}
}

func TestFileDescriptorTracking(t *testing.T) {
	ld := NewLeakDetector()
	ld.SetLeakThreshold(50 * time.Millisecond)
	
	testFd := &struct{ fd int }{fd: 123}
	_ = ld.TrackFileDescriptor(testFd, "/test/file", "test fd")
	
	time.Sleep(100 * time.Millisecond)
	
	report := ld.CheckForLeaks()
	if report.TotalLeaks == 0 {
		t.Fatal("Expected file descriptor leak")
	}
}

func TestMonitoringFunctionality(t *testing.T) {
	ld := NewLeakDetector()
	ld.SetLeakThreshold(50 * time.Millisecond)
	
	// Start monitoring
	ctx, cancel := context.WithCancel(context.Background())
	go ld.StartMonitoring(ctx)
	
	// Track a resource
	testIter := &struct{ name string }{name: "test"}
	ld.TrackIterator(testIter, "test iterator")
	
	// Wait for leak detection
	time.Sleep(100 * time.Millisecond)
	
	// Stop monitoring
	cancel()
	
	// Check that monitoring worked
	report := ld.CheckForLeaks()
	if report.TotalLeaks == 0 {
		t.Fatal("Expected at least one leak")
	}
}