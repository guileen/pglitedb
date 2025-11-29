package leak

import (
	"github.com/guileen/pglitedb/engine/pebble/leak_detection"
	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// Detector handles leak detection functionality
type Detector struct {
	leakDetector engineTypes.LeakDetector
}

// NewDetector creates a new leak detector
func NewDetector() *Detector {
	ld := leak_detection.NewLeakDetector()
	return &Detector{
		leakDetector: ld,
	}
}

// StartMonitoring starts leak detection monitoring
func (d *Detector) StartMonitoring() {
	// Start leak detection monitoring
}

// TrackTransaction tracks a transaction for leak detection
func (d *Detector) TrackTransaction(txn interface{}) {
	if d.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		d.leakDetector.TrackTransaction(txn, stackTrace)
	}
}

// TrackConnection tracks a connection for leak detection
func (d *Detector) TrackConnection(conn interface{}) {
	if d.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		d.leakDetector.TrackConnection(conn, stackTrace)
	}
}

// TrackFileDescriptor tracks a file descriptor for leak detection
func (d *Detector) TrackFileDescriptor(fd interface{}, path string) {
	if d.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		d.leakDetector.TrackFileDescriptor(fd, path, stackTrace)
	}
}

// TrackGoroutine tracks a goroutine for leak detection
func (d *Detector) TrackGoroutine(goroutineID uint64) {
	if d.leakDetector != nil {
		stackTrace := leak_detection.GetStackTrace()
		d.leakDetector.TrackGoroutine(goroutineID, stackTrace)
	}
}

// TrackCurrentGoroutine tracks the current goroutine for leak detection
func (d *Detector) TrackCurrentGoroutine() {
	goroutineID := leak_detection.GetCurrentGoroutineID()
	d.TrackGoroutine(goroutineID)
}

// CheckForLeaks checks for resource leaks and returns a report
func (d *Detector) CheckForLeaks() *engineTypes.LeakReport {
	if d.leakDetector != nil {
		return d.leakDetector.CheckForLeaks()
	}
	return &engineTypes.LeakReport{}
}

// GetLeakDetector returns the leak detector
func (d *Detector) GetLeakDetector() engineTypes.LeakDetector {
	return d.leakDetector
}