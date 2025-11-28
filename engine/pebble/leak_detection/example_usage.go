// Package leak_detection provides resource leak detection capabilities
package leak_detection

// ExampleLeakDetection demonstrates how to use the leak detection system
/*
func ExampleLeakDetection() {
	// Get the resource manager
	rm := pebble.GetResourceManager()
	
	// Set a leak threshold (optional, defaults to 5 minutes)
	if leakDetector := rm.GetLeakDetector(); leakDetector != nil {
		leakDetector.SetLeakThreshold(10 * time.Minute)
	}
	
	// Start monitoring for leaks (optional, runs in background)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	if leakDetector := rm.GetLeakDetector(); leakDetector != nil {
		leakDetector.StartMonitoring(ctx)
	}
	
	// Use resources normally
	iter := rm.AcquireIterator()
	
	// ... do work with iterator ...
	
	// Properly close the iterator to avoid leaks
	iter.Close()
	
	// Check for leaks manually (optional)
	report := rm.CheckForLeaks()
	if report.TotalLeaks > 0 {
		fmt.Printf("Detected %d resource leaks:\n", report.TotalLeaks)
		for _, leak := range report.Leaks {
			fmt.Printf("  - %s (%s): %v\n", leak.ResourceID, leak.ResourceType, leak.LeakDuration)
		}
	}
	
	// Get leak detection metrics
	if leakDetector := rm.GetLeakDetector(); leakDetector != nil {
		metrics := leakDetector.GetMetrics()
		fmt.Printf("Total tracked resources: %d\n", metrics.TotalTracked)
		fmt.Printf("Total leaks detected: %d\n", metrics.TotalLeaks)
		fmt.Printf("Active resources: %d\n", metrics.ActiveResources)
	}
}
*/