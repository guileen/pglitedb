package network

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// EnhancedConnectionPool provides improved connection pooling with better adaptive algorithms
type EnhancedConnectionPool struct {
	*ConnectionPool
}

// NewEnhancedConnectionPool creates a new enhanced connection pool
func NewEnhancedConnectionPool(config PoolConfig, factory ConnectionFactory) *EnhancedConnectionPool {
	// Set enhanced defaults for better performance
	if config.AdaptationInterval <= 0 {
		config.AdaptationInterval = 15 * time.Second // More frequent adaptation
	}
	if config.HealthCheckPeriod <= 0 {
		config.HealthCheckPeriod = 30 * time.Second // More frequent health checks
	}
	
	// Set default adaptive pooling parameters if not configured
	if config.TargetHitRate <= 0 {
		config.TargetHitRate = 95.0 // Aim for 95% hit rate
	}
	if config.MinHitRateThreshold <= 0 {
		config.MinHitRateThreshold = 85.0 // Expand if below 85%
	}
	if config.MaxHitRateThreshold <= 0 {
		config.MaxHitRateThreshold = 98.0 // Contract if above 98%
	}
	if config.ExpansionFactor <= 0 {
		config.ExpansionFactor = 1.25 // 25% expansion
	}
	if config.ContractionFactor <= 0 {
		config.ContractionFactor = 0.9 // 10% contraction
	}
	if config.MaxAdaptiveConnections <= 0 {
		config.MaxAdaptiveConnections = config.MaxConnections * 2 // Double max for expansion
	}
	if config.MinAdaptiveConnections <= 0 {
		config.MinAdaptiveConnections = config.MinConnections // Don't go below min
	}
	
	pool := NewConnectionPool(config, factory)
	return &EnhancedConnectionPool{
		ConnectionPool: pool,
	}
}

// GetWithMultiplexing retrieves a connection with multiplexing support
func (ep *EnhancedConnectionPool) GetWithMultiplexing(ctx context.Context, multiplexKey string) (*PooledConnection, error) {
	// For now, delegate to regular Get
	// Future enhancement could implement connection sharing based on multiplexKey
	return ep.ConnectionPool.Get(ctx)
}

// GetEnhancedMetrics returns detailed metrics including predictive analytics
func (ep *EnhancedConnectionPool) GetEnhancedMetrics() EnhancedPoolMetrics {
	baseMetrics := ep.GetMetrics()
	
	// Get adaptive-specific information
	history := ep.GetHitRateHistory()
	historyLen := len(history)
	var avgHitRate float64
	if historyLen > 0 {
		for _, rate := range history {
			avgHitRate += rate
		}
		avgHitRate /= float64(historyLen)
	}
	lastAdaptation := ep.GetLastAdaptationTime()
	
	current, max := ep.GetPoolCapacity()
	
	// Calculate error rate from pool stats
	ep.mu.Lock()
	errorCount := ep.stats.ConnectionErrors
	totalConnections := ep.stats.TotalConns
	ep.mu.Unlock()
	
	errorRate := 0.0
	if totalConnections > 0 {
		errorRate = float64(errorCount) / float64(totalConnections) * 100
	}
	
	return EnhancedPoolMetrics{
		PoolMetrics:             baseMetrics,
		CurrentPoolSize:         current,
		MaxPoolSize:             max,
		AverageHitRate:          avgHitRate,
		HitRateHistoryLength:    historyLen,
		TimeSinceLastAdapt:      time.Since(lastAdaptation),
		AdaptivePoolingEnabled:  ep.IsAdaptivePoolingEnabled(),
		ErrorRate:               errorRate,
		PredictedOptimalSize:    ep.predictOptimalPoolSize(),
	}
}

// predictOptimalPoolSize uses historical data to predict optimal pool size
func (ep *EnhancedConnectionPool) predictOptimalPoolSize() int {
	history := ep.GetHitRateHistory()
	if len(history) < 3 {
		return ep.config.MaxConnections // Not enough data
	}
	
	// Simple prediction: if trend is decreasing, increase pool size
	latest := history[len(history)-1]
	previous := history[len(history)-2]
	trend := latest - previous
	
	currentMax := ep.config.MaxConnections
	
	// If hit rate is trending down and we're below target, expand
	if trend < 0 && latest < ep.config.TargetHitRate {
		newSize := int(float64(currentMax) * 1.1) // 10% increase
		if newSize > ep.config.MaxAdaptiveConnections {
			newSize = ep.config.MaxAdaptiveConnections
		}
		return newSize
	}
	
	// If hit rate is stable and above max threshold, contract
	if abs(trend) < 1.0 && latest > ep.config.MaxHitRateThreshold {
		newSize := int(float64(currentMax) * 0.95) // 5% decrease
		if newSize < ep.config.MinAdaptiveConnections {
			newSize = ep.config.MinAdaptiveConnections
		}
		return newSize
	}
	
	return currentMax
}

// EnhancedPoolMetrics extends PoolMetrics with enhanced information
type EnhancedPoolMetrics struct {
	PoolMetrics
	
	// Adaptive pooling metrics
	CurrentPoolSize       int
	MaxPoolSize           int
	AverageHitRate        float64
	HitRateHistoryLength  int
	TimeSinceLastAdapt    time.Duration
	AdaptivePoolingEnabled bool
	ErrorRate             float64
	PredictedOptimalSize  int
}

// abs returns the absolute value of a float64
func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

// ConnectionMultiplexer provides connection sharing capabilities
type ConnectionMultiplexer struct {
	pool      *EnhancedConnectionPool
	sharedConns map[string]*SharedConnection
	mu        sync.RWMutex
}

// SharedConnection wraps a pooled connection for sharing
type SharedConnection struct {
	*PooledConnection
	refCount int32
	mu       sync.Mutex
}

// NewConnectionMultiplexer creates a new connection multiplexer
func NewConnectionMultiplexer(pool *EnhancedConnectionPool) *ConnectionMultiplexer {
	return &ConnectionMultiplexer{
		pool:        pool,
		sharedConns: make(map[string]*SharedConnection),
	}
}

// GetSharedConnection retrieves a shared connection
func (cm *ConnectionMultiplexer) GetSharedConnection(ctx context.Context, key string) (*SharedConnection, error) {
	cm.mu.RLock()
	if shared, exists := cm.sharedConns[key]; exists && atomic.LoadInt32(&shared.refCount) > 0 {
		atomic.AddInt32(&shared.refCount, 1)
		cm.mu.RUnlock()
		return shared, nil
	}
	cm.mu.RUnlock()
	
	// Create new shared connection
	conn, err := cm.pool.Get(ctx)
	if err != nil {
		return nil, err
	}
	
	shared := &SharedConnection{
		PooledConnection: conn,
		refCount:         1,
	}
	
	cm.mu.Lock()
	cm.sharedConns[key] = shared
	cm.mu.Unlock()
	
	return shared, nil
}

// ReleaseSharedConnection releases a shared connection
func (cm *ConnectionMultiplexer) ReleaseSharedConnection(key string, shared *SharedConnection) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	
	newRefCount := atomic.AddInt32(&shared.refCount, -1)
	if newRefCount <= 0 {
		// No more references, return to pool
		shared.PooledConnection.pool.Put(shared.PooledConnection)
		delete(cm.sharedConns, key)
	}
}

// PoolOptimizer provides advanced pool optimization capabilities
type PoolOptimizer struct {
	pool *EnhancedConnectionPool
}

// NewPoolOptimizer creates a new pool optimizer
func NewPoolOptimizer(pool *EnhancedConnectionPool) *PoolOptimizer {
	return &PoolOptimizer{
		pool: pool,
	}
}

// OptimizePool dynamically optimizes the pool based on current workload
func (po *PoolOptimizer) OptimizePool() {
	// Get current metrics
	metrics := po.pool.GetEnhancedMetrics()
	
	// If error rate is high, consider reducing pool size to reduce load
	if metrics.ErrorRate > 5.0 { // 5% error rate is high
		currentMax := po.pool.config.MaxConnections
		newMax := int(float64(currentMax) * 0.9) // Reduce by 10%
		if newMax >= po.pool.config.MinAdaptiveConnections {
			po.pool.ConnectionPool.AdjustPoolCapacity(newMax)
		}
		return
	}
	
	// Use predictive algorithm to determine optimal size
	predictedSize := po.pool.predictOptimalPoolSize()
	currentMax := po.pool.config.MaxConnections
	
	if predictedSize != currentMax {
		po.pool.ConnectionPool.AdjustPoolCapacity(predictedSize)
	}
}

// GetOptimizationRecommendations returns recommendations for pool optimization
func (po *PoolOptimizer) GetOptimizationRecommendations() []string {
	var recommendations []string
	
	metrics := po.pool.GetEnhancedMetrics()
	
	// Hit rate recommendations
	if metrics.AverageHitRate < po.pool.config.MinHitRateThreshold {
		recommendations = append(recommendations, 
			fmt.Sprintf("Hit rate (%.2f%%) below threshold (%.2f%%), consider expanding pool", 
				metrics.AverageHitRate, po.pool.config.MinHitRateThreshold))
	} else if metrics.AverageHitRate > po.pool.config.MaxHitRateThreshold {
		recommendations = append(recommendations,
			fmt.Sprintf("Hit rate (%.2f%%) above threshold (%.2f%%), consider contracting pool",
				metrics.AverageHitRate, po.pool.config.MaxHitRateThreshold))
	}
	
	// Error rate recommendations
	if metrics.ErrorRate > 5.0 {
		recommendations = append(recommendations,
			fmt.Sprintf("High error rate (%.2f%%), consider reducing pool size or checking backend health",
				metrics.ErrorRate))
	}
	
	// Pool size recommendations
	if float64(metrics.CurrentPoolSize) < float64(metrics.MaxPoolSize)*0.3 {
		recommendations = append(recommendations,
			"Pool utilization low, consider reducing maximum pool size to save resources")
	}
	
	return recommendations
}