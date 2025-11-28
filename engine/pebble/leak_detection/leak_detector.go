package leak_detection

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// leakDetector implements LeakDetector interface
type leakDetector struct {
	trackedResources sync.Map // map[string]*trackedResource
	leakThreshold    time.Duration
	metrics          leakMetrics
	monitoringCtx    context.Context
	monitoringCancel context.CancelFunc
	monitoringWg     sync.WaitGroup
	isMonitoring     int32 // atomic flag
}

// NewLeakDetector creates a new leak detector
func NewLeakDetector() engineTypes.LeakDetector {
	ld := &leakDetector{
		leakThreshold: 5 * time.Minute, // Default 5 minute threshold
		metrics:       newLeakMetrics(),
	}
	return ld
}

// TrackIterator tracks an iterator for leak detection
func (ld *leakDetector) TrackIterator(iter interface{}, stackTrace string) engineTypes.TrackedResource {
	id := fmt.Sprintf("iter_%p", iter)
	resource := newTrackedResource(id, engineTypes.ResourceTypeIterator, stackTrace, ld.leakThreshold)
	ld.trackedResources.Store(id, resource)
	ld.metrics.incrementTracked(engineTypes.ResourceTypeIterator)
	return resource
}

// TrackTransaction tracks a transaction for leak detection
func (ld *leakDetector) TrackTransaction(txn interface{}, stackTrace string) engineTypes.TrackedResource {
	id := fmt.Sprintf("txn_%p", txn)
	resource := newTrackedResource(id, engineTypes.ResourceTypeTransaction, stackTrace, ld.leakThreshold)
	ld.trackedResources.Store(id, resource)
	ld.metrics.incrementTracked(engineTypes.ResourceTypeTransaction)
	return resource
}

// TrackConnection tracks a connection for leak detection
func (ld *leakDetector) TrackConnection(conn interface{}, stackTrace string) engineTypes.TrackedResource {
	id := fmt.Sprintf("conn_%p", conn)
	resource := newTrackedResource(id, engineTypes.ResourceTypeConnection, stackTrace, ld.leakThreshold)
	ld.trackedResources.Store(id, resource)
	ld.metrics.incrementTracked(engineTypes.ResourceTypeConnection)
	return resource
}

// TrackFileDescriptor tracks a file descriptor for leak detection
func (ld *leakDetector) TrackFileDescriptor(fd interface{}, path string, stackTrace string) engineTypes.TrackedResource {
	id := fmt.Sprintf("fd_%p", fd)
	resource := newTrackedResource(id, engineTypes.ResourceTypeFileDescriptor, stackTrace, ld.leakThreshold)
	ld.trackedResources.Store(id, resource)
	ld.metrics.incrementTracked(engineTypes.ResourceTypeFileDescriptor)
	return resource
}

// TrackGoroutine tracks a goroutine for leak detection
func (ld *leakDetector) TrackGoroutine(goroutineID uint64, stackTrace string) engineTypes.TrackedResource {
	id := fmt.Sprintf("goroutine_%d", goroutineID)
	resource := newTrackedResource(id, engineTypes.ResourceTypeGoroutine, stackTrace, ld.leakThreshold)
	ld.trackedResources.Store(id, resource)
	ld.metrics.incrementTracked(engineTypes.ResourceTypeGoroutine)
	return resource
}

// CheckForLeaks checks for resource leaks and returns a report
func (ld *leakDetector) CheckForLeaks() *engineTypes.LeakReport {
	report := &engineTypes.LeakReport{
		Timestamp:     time.Now(),
		Leaks:         make([]engineTypes.LeakInfo, 0),
		ResourceStats: make(map[engineTypes.ResourceType]int),
	}
	
	totalLeaks := 0
	
	ld.trackedResources.Range(func(key, value interface{}) bool {
		resource := value.(*trackedResource)
		resourceType := resource.GetResourceType()
		
		// Update resource stats
		report.ResourceStats[resourceType]++
		
		// Check if resource is leaked
		if resource.IsLeaked() {
			leakInfo := engineTypes.LeakInfo{
				ResourceID:    resource.GetID(),
				ResourceType:  resource.GetResourceType(),
				StackTrace:    resource.GetStackTrace(),
				AllocationTime: resource.GetAllocationTime(),
				LeakDuration:  time.Since(resource.GetAllocationTime()),
			}
			report.Leaks = append(report.Leaks, leakInfo)
			totalLeaks++
			ld.metrics.incrementLeaks(resourceType)
		} else if atomic.LoadInt32(&resource.released) == 1 {
			// Resource was properly released, remove it from tracking
			ld.trackedResources.Delete(key)
			ld.metrics.decrementActive(resourceType)
		}
		
		return true
	})
	
	report.TotalLeaks = totalLeaks
	ld.metrics.setTotalLeaks(uint64(totalLeaks))
	
	return report
}

// StartMonitoring starts the leak detection monitoring
func (ld *leakDetector) StartMonitoring(ctx context.Context) {
	if !atomic.CompareAndSwapInt32(&ld.isMonitoring, 0, 1) {
		// Already monitoring
		return
	}
	
	ld.monitoringCtx, ld.monitoringCancel = context.WithCancel(ctx)
	ld.monitoringWg.Add(1)
	
	go func() {
		defer ld.monitoringWg.Done()
		ticker := time.NewTicker(30 * time.Second) // Check every 30 seconds
		defer ticker.Stop()
		
		for {
			select {
			case <-ld.monitoringCtx.Done():
				return
			case <-ticker.C:
				// Perform leak check
				report := ld.CheckForLeaks()
				if report.TotalLeaks > 0 {
					// Log or handle leaks (in a real implementation, this might log to a file or send alerts)
					fmt.Printf("LEAK DETECTED: %d resources leaked\n", report.TotalLeaks)
					for _, leak := range report.Leaks {
						fmt.Printf("  - %s (%s): Leaked for %v\n", leak.ResourceID, leak.ResourceType, leak.LeakDuration)
					}
				}
			}
		}
	}()
}

// StopMonitoring stops the leak detection monitoring
func (ld *leakDetector) StopMonitoring() {
	if !atomic.CompareAndSwapInt32(&ld.isMonitoring, 1, 0) {
		// Not monitoring
		return
	}
	
	if ld.monitoringCancel != nil {
		ld.monitoringCancel()
	}
	ld.monitoringWg.Wait()
}

// SetLeakThreshold sets the threshold for leak detection
func (ld *leakDetector) SetLeakThreshold(threshold time.Duration) {
	ld.leakThreshold = threshold
}

// GetMetrics returns leak detection metrics
func (ld *leakDetector) GetMetrics() engineTypes.LeakMetrics {
	return ld.metrics.getMetrics()
}

// GetStackTrace captures the current stack trace
func GetStackTrace() string {
	buf := make([]byte, 4096)
	n := runtime.Stack(buf, false)
	
	// Parse the stack trace to remove internal leak detection frames
	stack := string(buf[:n])
	lines := strings.Split(stack, "\n")
	
	// Filter out leak detection internal frames
	filteredLines := make([]string, 0, len(lines))
	skipNext := false
	
	for _, line := range lines {
		// Skip empty lines
		if line == "" {
			filteredLines = append(filteredLines, line)
			continue
		}
		
		// Skip leak detection internal frames
		if strings.Contains(line, "leak_detection") && 
		   (strings.Contains(line, "TrackIterator") || 
		    strings.Contains(line, "TrackTransaction") || 
		    strings.Contains(line, "TrackConnection") || 
		    strings.Contains(line, "TrackFileDescriptor") || 
		    strings.Contains(line, "TrackGoroutine")) {
			skipNext = true
			continue
		}
		
		// Skip the next line if we're skipping a frame
		if skipNext {
			skipNext = false
			continue
		}
		
		filteredLines = append(filteredLines, line)
	}
	
	return strings.Join(filteredLines, "\n")
}