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
func (sm *Manager) InitializeDefaultSizes() {
	sm.poolAdjustmentMu.Lock()
	defer sm.poolAdjustmentMu.Unlock()
	
	sm.poolSizes["iterator"] = 500
	sm.poolSizes["batch"] = 500
	sm.poolSizes["txn"] = 500
	sm.poolSizes["record"] = 5000
	sm.poolSizes["buffer"] = 5000
	sm.poolSizes["keyEncoder"] = 500
	sm.poolSizes["filterExpr"] = 500
	sm.poolSizes["scanResult"] = 500
	sm.poolSizes["indexKey"] = 500
	sm.poolSizes["tableKey"] = 500
	sm.poolSizes["metaKey"] = 500
	sm.poolSizes["compositeKey"] = 500
}

// AdjustPoolSizes adapts pool sizes based on hit rates and usage patterns
func (sm *Manager) AdjustPoolSizes(metricsCollector *metrics.Collector) {
	sm.poolAdjustmentMu.Lock()
	defer sm.poolAdjustmentMu.Unlock()
	
	// Get actual metrics from the collector
	metricsData := metricsCollector.GetMetrics()
	
	// Calculate hit rates based on actual metrics
	// This is a simplified example - in practice, you'd want more sophisticated logic
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
	for poolName, hitRate := range sm.poolHitRates {
		currentSize := sm.poolSizes[poolName]
		if hitRate < 0.7 && currentSize < 50000 { // Increase pool size more aggressively if hit rate is low
			newSize := int(float64(currentSize) * 2.5)
			if newSize > 50000 {
				newSize = 50000
			}
			sm.poolSizes[poolName] = newSize
		} else if hitRate > 0.95 && currentSize > 50 { // Decrease pool size if hit rate is very high
			sm.poolSizes[poolName] = currentSize / 2
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