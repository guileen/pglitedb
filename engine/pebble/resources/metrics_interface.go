package resources

import (
	"github.com/guileen/pglitedb/engine/pebble/resources/metrics"
)

// GetResourceMetrics returns the current resource metrics
func (rm *ResourceManager) GetResourceMetrics() metrics.Metrics {
	return rm.metrics.GetMetrics()
}

// ResetResourceMetrics resets all resource metrics
func (rm *ResourceManager) ResetResourceMetrics() {
	rm.metrics.ResetMetrics()
}