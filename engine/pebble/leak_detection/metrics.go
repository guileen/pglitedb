package leak_detection

import (
	"sync/atomic"

	engineTypes "github.com/guileen/pglitedb/engine/types"
)

// leakMetrics tracks leak detection metrics
type leakMetrics struct {
	totalTracked    uint64
	totalLeaks      uint64
	activeResources map[engineTypes.ResourceType]*resourceTypeMetrics
}

// resourceTypeMetrics tracks metrics for a specific resource type
type resourceTypeMetrics struct {
	tracked uint64
	leaks   uint64
	active  uint64
}

// newLeakMetrics creates a new leak metrics tracker
func newLeakMetrics() leakMetrics {
	return leakMetrics{
		activeResources: make(map[engineTypes.ResourceType]*resourceTypeMetrics),
	}
}

// initializeResourceType initializes metrics for a resource type if not already present
func (lm *leakMetrics) initializeResourceType(resourceType engineTypes.ResourceType) *resourceTypeMetrics {
	if metrics, exists := lm.activeResources[resourceType]; exists {
		return metrics
	}
	
	metrics := &resourceTypeMetrics{}
	lm.activeResources[resourceType] = metrics
	return metrics
}

// incrementTracked increments the tracked counter for a resource type
func (lm *leakMetrics) incrementTracked(resourceType engineTypes.ResourceType) {
	atomic.AddUint64(&lm.totalTracked, 1)
	
	metrics := lm.initializeResourceType(resourceType)
	atomic.AddUint64(&metrics.tracked, 1)
	atomic.AddUint64(&metrics.active, 1)
}

// incrementLeaks increments the leaks counter for a resource type
func (lm *leakMetrics) incrementLeaks(resourceType engineTypes.ResourceType) {
	atomic.AddUint64(&lm.totalLeaks, 1)
	
	metrics := lm.initializeResourceType(resourceType)
	atomic.AddUint64(&metrics.leaks, 1)
	// Note: We don't decrement active here as the resource is still tracked
}

// decrementActive decrements the active counter for a resource type
func (lm *leakMetrics) decrementActive(resourceType engineTypes.ResourceType) {
	metrics := lm.initializeResourceType(resourceType)
	atomic.AddUint64(&metrics.active, ^uint64(0)) // Decrement by 1
}

// setTotalLeaks sets the total leaks counter
func (lm *leakMetrics) setTotalLeaks(leaks uint64) {
	atomic.StoreUint64(&lm.totalLeaks, leaks)
}

// getMetrics returns the current leak metrics
func (lm *leakMetrics) getMetrics() engineTypes.LeakMetrics {
	resourceTypes := make(map[engineTypes.ResourceType]engineTypes.ResourceTypeMetrics)
	
	for resourceType, metrics := range lm.activeResources {
		resourceTypes[resourceType] = engineTypes.ResourceTypeMetrics{
			Tracked: atomic.LoadUint64(&metrics.tracked),
			Leaks:   atomic.LoadUint64(&metrics.leaks),
			Active:  atomic.LoadUint64(&metrics.active),
		}
	}
	
	return engineTypes.LeakMetrics{
		TotalTracked:    atomic.LoadUint64(&lm.totalTracked),
		TotalLeaks:      atomic.LoadUint64(&lm.totalLeaks),
		ActiveResources: atomic.LoadUint64(&lm.totalTracked) - atomic.LoadUint64(&lm.totalLeaks),
		ResourceTypes:   resourceTypes,
	}
}