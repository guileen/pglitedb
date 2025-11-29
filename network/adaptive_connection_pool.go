// Package network provides adaptive connection pooling implementation for PostgreSQL connections
// with dynamic sizing based on usage patterns and metrics collection
package network

import (
	"context"
	"time"
)

// AdaptiveConnectionPool wraps ConnectionPool with adaptive sizing capabilities
type AdaptiveConnectionPool struct {
	*ConnectionPool
}

// NewAdaptiveConnectionPool creates a new adaptive connection pool
func NewAdaptiveConnectionPool(config PoolConfig, factory ConnectionFactory) *AdaptiveConnectionPool {
	// Enable adaptive pooling by default for this wrapper
	config.AdaptivePoolingEnabled = true
	
	pool := NewConnectionPool(config, factory)
	return &AdaptiveConnectionPool{
		ConnectionPool: pool,
	}
}

// Get acquires a connection from the pool, tracking hit rate for adaptation
func (ap *AdaptiveConnectionPool) Get(ctx context.Context) (*PooledConnection, error) {
	return ap.ConnectionPool.Get(ctx)
}

// Put returns a connection to the pool
func (ap *AdaptiveConnectionPool) Put(conn *PooledConnection) {
	ap.ConnectionPool.Put(conn)
}

// Close shuts down the connection pool gracefully
func (ap *AdaptiveConnectionPool) Close() error {
	return ap.ConnectionPool.Close()
}

// GetAdaptiveMetrics returns detailed metrics including adaptive pooling information
func (ap *AdaptiveConnectionPool) GetAdaptiveMetrics() AdaptivePoolMetrics {
	baseMetrics := ap.GetMetrics()
	
	// Get adaptive-specific information
	history := ap.GetHitRateHistory()
	historyLen := len(history)
	var avgHitRate float64
	if historyLen > 0 {
		for _, rate := range history {
			avgHitRate += rate
		}
		avgHitRate /= float64(historyLen)
	}
	lastAdaptation := ap.GetLastAdaptationTime()
	
	current, max := ap.GetPoolCapacity()
	
	return AdaptivePoolMetrics{
		PoolMetrics:             baseMetrics,
		CurrentPoolSize:         current,
		MaxPoolSize:             max,
		AverageHitRate:          avgHitRate,
		HitRateHistoryLength:    historyLen,
		TimeSinceLastAdapt:      time.Since(lastAdaptation),
		AdaptivePoolingEnabled:  ap.IsAdaptivePoolingEnabled(),
	}
}

// ForceAdaptation triggers an immediate adaptation of the pool size
func (ap *AdaptiveConnectionPool) ForceAdaptation() {
	ap.ConnectionPool.ForceAdaptation()
}

// AdaptivePoolMetrics extends PoolMetrics with adaptive pooling information
type AdaptivePoolMetrics struct {
	PoolMetrics
	
	// Adaptive pooling metrics
	CurrentPoolSize       int
	MaxPoolSize           int
	AverageHitRate        float64
	HitRateHistoryLength  int
	TimeSinceLastAdapt    time.Duration
	AdaptivePoolingEnabled bool
}

// SetAdaptiveConfig updates the adaptive pooling configuration
func (ap *AdaptiveConnectionPool) SetAdaptiveConfig(target, minThreshold, maxThreshold float64) {
	ap.ConnectionPool.SetAdaptiveConfig(target, minThreshold, maxThreshold)
}

// EnableAdaptivePooling enables or disables adaptive pooling
func (ap *AdaptiveConnectionPool) EnableAdaptivePooling(enabled bool) {
	ap.ConnectionPool.EnableAdaptivePooling(enabled)
}

// GetCurrentHitRate returns the current hit rate of the pool
func (ap *AdaptiveConnectionPool) GetCurrentHitRate() float64 {
	metrics := ap.GetMetrics()
	return metrics.HitRate
}

// GetHitRateHistory returns the recent hit rate history
func (ap *AdaptiveConnectionPool) GetHitRateHistory() []float64 {
	return ap.ConnectionPool.GetHitRateHistory()
}