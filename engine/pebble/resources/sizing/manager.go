package sizing

import (
	"sync"

	"github.com/guileen/pglitedb/engine/pebble/resources/metrics"
)

// Manager handles adaptive pool sizing
type Manager struct {
	poolSizes        map[string]int     // Track current pool sizes
	poolHitRates     map[string]float64 // Track pool hit rates
	poolAdjustmentMu sync.RWMutex       // Mutex for pool adjustment operations
}

// NewManager creates a new sizing manager
func NewManager() *Manager {
	return &Manager{
		poolSizes:    make(map[string]int),
		poolHitRates: make(map[string]float64),
	}
}

// InitializeDefaultSizes initializes default pool sizes
// Based on historical performance report: Increased default pool sizes by 5x across all resource types
func (sm *Manager) InitializeDefaultSizes() {
	sm.poolAdjustmentMu.Lock()
	defer sm.poolAdjustmentMu.Unlock()
	
	sm.poolSizes["iterator"] = 2500  // 500 * 5
	sm.poolSizes["batch"] = 2500    // 500 * 5
	sm.poolSizes["txn"] = 2500      // 500 * 5
	sm.poolSizes["record"] = 25000  // 5000 * 5
	sm.poolSizes["buffer"] = 25000  // 5000 * 5
	sm.poolSizes["keyEncoder"] = 2500  // 500 * 5
	sm.poolSizes["filterExpr"] = 2500  // 500 * 5
	sm.poolSizes["scanResult"] = 2500  // 500 * 5
	sm.poolSizes["indexKey"] = 2500   // 500 * 5
	sm.poolSizes["tableKey"] = 2500   // 500 * 5
	sm.poolSizes["metaKey"] = 2500    // 500 * 5
	sm.poolSizes["compositeKey"] = 2500  // 500 * 5
}

// AdjustPoolSizes adapts pool sizes based on hit rates and usage patterns
// Based on historical performance report: Improved pool adjustment algorithm with more aggressive scaling
func (sm *Manager) AdjustPoolSizes(metricsCollector *metrics.Collector) {
	sm.poolAdjustmentMu.Lock()
	defer sm.poolAdjustmentMu.Unlock()
	
	// Get actual metrics from the collector
	metricsData := metricsCollector.GetMetrics()
	
	// Calculate hit rates based on actual metrics
	if metricsData.IteratorAcquired > 0 {
		hitRate := float64(metricsData.IteratorReleased) / float64(metricsData.IteratorAcquired)
		sm.poolHitRates["iterator"] = hitRate
	}
	
	if metricsData.BatchAcquired > 0 {
		hitRate := float64(metricsData.BatchReleased) / float64(metricsData.BatchAcquired)
		sm.poolHitRates["batch"] = hitRate
	}
	
	if metricsData.TxnAcquired > 0 {
		hitRate := float64(metricsData.TxnReleased) / float64(metricsData.TxnAcquired)
		sm.poolHitRates["txn"] = hitRate
	}
	
	if metricsData.RecordAcquired > 0 {
		hitRate := float64(metricsData.RecordReleased) / float64(metricsData.RecordAcquired)
		sm.poolHitRates["record"] = hitRate
	}
	
	if metricsData.BufferAcquired > 0 {
		hitRate := float64(metricsData.BufferReleased) / float64(metricsData.BufferAcquired)
		sm.poolHitRates["buffer"] = hitRate
	}
	
	// Adjust pool sizes based on hit rates with more aggressive scaling
	// Based on historical performance report: Better hit rate tracking and pool size adjustments
	for poolName, hitRate := range sm.poolHitRates {
		currentSize := sm.poolSizes[poolName]
		if hitRate < 0.8 && currentSize < 100000 { // Increase pool size more aggressively if hit rate is low
			// More aggressive expansion - increase by 3x when hit rate is below 80%
			newSize := int(float64(currentSize) * 3.0)
			if newSize > 100000 {
				newSize = 100000
			}
			sm.poolSizes[poolName] = newSize
			// Log the adjustment for monitoring
		} else if hitRate > 0.98 && currentSize > 100 { // Decrease pool size if hit rate is very high
			// More conservative contraction - only reduce by 25% when hit rate is extremely high
			sm.poolSizes[poolName] = int(float64(currentSize) * 0.75)
		}
	}
}

// GetPoolHitRate returns the current hit rate for a specific pool
func (sm *Manager) GetPoolHitRate(poolName string) float64 {
	sm.poolAdjustmentMu.RLock()
	defer sm.poolAdjustmentMu.RUnlock()
	
	if hitRate, exists := sm.poolHitRates[poolName]; exists {
		return hitRate
	}
	return 0.0
}

// SetPoolSize manually sets the size for a specific pool
func (sm *Manager) SetPoolSize(poolName string, size int) {
	sm.poolAdjustmentMu.Lock()
	defer sm.poolAdjustmentMu.Unlock()
	
	sm.poolSizes[poolName] = size
}

// GetPoolSize returns the current size for a specific pool
func (sm *Manager) GetPoolSize(poolName string) int {
	sm.poolAdjustmentMu.RLock()
	defer sm.poolAdjustmentMu.RUnlock()
	
	if size, exists := sm.poolSizes[poolName]; exists {
		return size
	}
	return 0
}