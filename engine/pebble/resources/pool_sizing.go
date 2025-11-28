package resources

// adjustPoolSizes adapts pool sizes based on hit rates and usage patterns
func (rm *ResourceManager) adjustPoolSizes() {
	rm.poolAdjustmentMu.Lock()
	defer rm.poolAdjustmentMu.Unlock()
	
	// Get current metrics
	metrics := rm.metrics.GetMetrics()
	
	// Calculate hit rates for major pools
	if metrics.IteratorAcquired > 0 {
		hitRate := float64(metrics.PoolHits) / float64(metrics.IteratorAcquired)
		rm.poolHitRates["iterator"] = hitRate
		
		// Adjust pool size based on hit rate
		if hitRate < 0.8 { // Low hit rate, increase pool size
			rm.poolSizes["iterator"] = int(float64(rm.poolSizes["iterator"]) * 1.2)
		} else if hitRate > 0.95 { // High hit rate, pool might be too large
			rm.poolSizes["iterator"] = int(float64(rm.poolSizes["iterator"]) * 0.95)
		}
	}
	
	// Similar adjustments for other pools
	if metrics.BatchAcquired > 0 {
		hitRate := float64(metrics.PoolHits) / float64(metrics.BatchAcquired)
		rm.poolHitRates["batch"] = hitRate
		
		if hitRate < 0.8 {
			rm.poolSizes["batch"] = int(float64(rm.poolSizes["batch"]) * 1.2)
		} else if hitRate > 0.95 {
			rm.poolSizes["batch"] = int(float64(rm.poolSizes["batch"]) * 0.95)
		}
	}
	
	if metrics.TxnAcquired > 0 {
		hitRate := float64(metrics.PoolHits) / float64(metrics.TxnAcquired)
		rm.poolHitRates["txn"] = hitRate
		
		if hitRate < 0.8 {
			rm.poolSizes["txn"] = int(float64(rm.poolSizes["txn"]) * 1.2)
		} else if hitRate > 0.95 {
			rm.poolSizes["txn"] = int(float64(rm.poolSizes["txn"]) * 0.95)
		}
	}
	
	// Ensure minimum pool sizes
	for poolName, size := range rm.poolSizes {
		if size < 10 {
			rm.poolSizes[poolName] = 10
		}
	}
}

// GetPoolHitRate returns the current hit rate for a specific pool
func (rm *ResourceManager) GetPoolHitRate(poolName string) float64 {
	rm.poolAdjustmentMu.RLock()
	defer rm.poolAdjustmentMu.RUnlock()
	
	if hitRate, exists := rm.poolHitRates[poolName]; exists {
		return hitRate
	}
	return 0.0
}

// SetPoolSize manually sets the size for a specific pool
func (rm *ResourceManager) SetPoolSize(poolName string, size int) {
	rm.poolAdjustmentMu.Lock()
	defer rm.poolAdjustmentMu.Unlock()
	
	rm.poolSizes[poolName] = size
}