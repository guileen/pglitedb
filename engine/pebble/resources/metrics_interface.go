package resources

// GetResourceMetrics returns the current resource metrics
func (rm *ResourceManager) GetResourceMetrics() ResourceMetrics {
	return rm.metrics.GetMetrics()
}

// ResetResourceMetrics resets all resource metrics
func (rm *ResourceManager) ResetResourceMetrics() {
	rm.metrics.ResetMetrics()
}