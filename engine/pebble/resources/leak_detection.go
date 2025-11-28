package resources

import (
	"github.com/guileen/pglitedb/engine/pebble/leak_detection"
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// TrackTransaction tracks a transaction for leak detection
func (rm *ResourceManager) TrackTransaction(txn interface{}) {
	if rm.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		rm.leakDetector.TrackTransaction(txn, stackTrace)
	}
}

// TrackConnection tracks a connection for leak detection
func (rm *ResourceManager) TrackConnection(conn interface{}) {
	if rm.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		rm.leakDetector.TrackConnection(conn, stackTrace)
	}
}

// TrackFileDescriptor tracks a file descriptor for leak detection
func (rm *ResourceManager) TrackFileDescriptor(fd interface{}, path string) {
	if rm.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		rm.leakDetector.TrackFileDescriptor(fd, path, stackTrace)
	}
}

// TrackGoroutine tracks a goroutine for leak detection
func (rm *ResourceManager) TrackGoroutine(goroutineID uint64) {
	if rm.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		rm.leakDetector.TrackGoroutine(goroutineID, stackTrace)
	}
}

// TrackCurrentGoroutine tracks the current goroutine for leak detection
func (rm *ResourceManager) TrackCurrentGoroutine() {
	goroutineID := leak_detection.GetCurrentGoroutineID()
	rm.TrackGoroutine(goroutineID)
}

// CheckForLeaks checks for resource leaks and returns a report
func (rm *ResourceManager) CheckForLeaks() *engineTypes.LeakReport {
	if rm.leakDetector != nil {
		return rm.leakDetector.CheckForLeaks()
	}
	return &engineTypes.LeakReport{}
}

// GetLeakDetector returns the leak detector
func (rm *ResourceManager) GetLeakDetector() engineTypes.LeakDetector {
	return rm.leakDetector
}