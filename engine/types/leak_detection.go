package types

import (
	"context"
	"time"
)

// LeakDetector defines the interface for resource leak detection
type LeakDetector interface {
	// TrackIterator tracks an iterator for leak detection
	TrackIterator(iter interface{}, stackTrace string) TrackedResource
	
	// TrackTransaction tracks a transaction for leak detection
	TrackTransaction(txn interface{}, stackTrace string) TrackedResource
	
	// TrackConnection tracks a connection for leak detection
	TrackConnection(conn interface{}, stackTrace string) TrackedResource
	
	// TrackFileDescriptor tracks a file descriptor for leak detection
	TrackFileDescriptor(fd interface{}, path string, stackTrace string) TrackedResource
	
	// TrackGoroutine tracks a goroutine for leak detection
	TrackGoroutine(goroutineID uint64, stackTrace string) TrackedResource
	
	// CheckForLeaks checks for resource leaks and returns a report
	CheckForLeaks() *LeakReport
	
	// StartMonitoring starts the leak detection monitoring
	StartMonitoring(ctx context.Context)
	
	// StopMonitoring stops the leak detection monitoring
	StopMonitoring()
	
	// SetLeakThreshold sets the threshold for leak detection
	SetLeakThreshold(threshold time.Duration)
	
	// GetMetrics returns leak detection metrics
	GetMetrics() LeakMetrics
}

// TrackedResource represents a tracked resource
type TrackedResource interface {
	// GetID returns the resource ID
	GetID() string
	
	// GetResourceType returns the resource type
	GetResourceType() ResourceType
	
	// GetStackTrace returns the stack trace where the resource was allocated
	GetStackTrace() string
	
	// GetAllocationTime returns when the resource was allocated
	GetAllocationTime() time.Time
	
	// IsLeaked checks if the resource is leaked
	IsLeaked() bool
	
	// MarkReleased marks the resource as released
	MarkReleased()
	
	// Close releases the resource tracking
	Close() error
}

// ResourceType represents the type of resource being tracked
type ResourceType string

const (
	ResourceTypeIterator      ResourceType = "iterator"
	ResourceTypeTransaction   ResourceType = "transaction"
	ResourceTypeConnection    ResourceType = "connection"
	ResourceTypeFileDescriptor ResourceType = "file_descriptor"
	ResourceTypeGoroutine     ResourceType = "goroutine"
)

// LeakReport represents a leak detection report
type LeakReport struct {
	Timestamp     time.Time              `json:"timestamp"`
	Leaks         []LeakInfo             `json:"leaks"`
	TotalLeaks    int                    `json:"total_leaks"`
	ResourceStats map[ResourceType]int   `json:"resource_stats"`
}

// LeakInfo contains information about a detected leak
type LeakInfo struct {
	ResourceID    string        `json:"resource_id"`
	ResourceType  ResourceType  `json:"resource_type"`
	StackTrace    string        `json:"stack_trace"`
	AllocationTime time.Time    `json:"allocation_time"`
	LeakDuration  time.Duration `json:"leak_duration"`
}

// LeakMetrics contains metrics about leak detection
type LeakMetrics struct {
	TotalTracked    uint64            `json:"total_tracked"`
	TotalLeaks      uint64            `json:"total_leaks"`
	ActiveResources uint64            `json:"active_resources"`
	ResourceTypes   map[ResourceType]ResourceTypeMetrics `json:"resource_types"`
}

// ResourceTypeMetrics contains metrics for a specific resource type
type ResourceTypeMetrics struct {
	Tracked   uint64 `json:"tracked"`
	Leaks     uint64 `json:"leaks"`
	Active    uint64 `json:"active"`
}