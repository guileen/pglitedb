package leak_detection

import (
	"sync"
	"sync/atomic"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// trackedResource implements TrackedResource interface
type trackedResource struct {
	id             string
	resourceType   engineTypes.ResourceType
	stackTrace     string
	allocationTime time.Time
	leakThreshold  time.Duration
	released       int32 // atomic flag
	closeOnce      sync.Once
}

// newTrackedResource creates a new tracked resource
func newTrackedResource(
	id string,
	resourceType engineTypes.ResourceType,
	stackTrace string,
	leakThreshold time.Duration,
) *trackedResource {
	return &trackedResource{
		id:             id,
		resourceType:   resourceType,
		stackTrace:     stackTrace,
		allocationTime: time.Now(),
		leakThreshold:  leakThreshold,
		released:       0,
	}
}

// GetID returns the resource ID
func (tr *trackedResource) GetID() string {
	return tr.id
}

// GetResourceType returns the resource type
func (tr *trackedResource) GetResourceType() engineTypes.ResourceType {
	return tr.resourceType
}

// GetStackTrace returns the stack trace where the resource was allocated
func (tr *trackedResource) GetStackTrace() string {
	return tr.stackTrace
}

// GetAllocationTime returns when the resource was allocated
func (tr *trackedResource) GetAllocationTime() time.Time {
	return tr.allocationTime
}

// IsLeaked checks if the resource is leaked
func (tr *trackedResource) IsLeaked() bool {
	// If already released, it's not leaked
	if atomic.LoadInt32(&tr.released) == 1 {
		return false
	}
	
	// Check if it has exceeded the leak threshold
	return time.Since(tr.allocationTime) > tr.leakThreshold
}

// MarkReleased marks the resource as released
func (tr *trackedResource) MarkReleased() {
	atomic.StoreInt32(&tr.released, 1)
}

// Close releases the resource tracking
func (tr *trackedResource) Close() error {
	tr.closeOnce.Do(func() {
		tr.MarkReleased()
	})
	return nil
}