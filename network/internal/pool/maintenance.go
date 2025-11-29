// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import (
	"sync/atomic"
	"time"
)

// maintenance performs periodic maintenance tasks
func (p *ConnectionPool) maintenance() {
	ticker := time.NewTicker(p.config.HealthCheckPeriod)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.healthCheck()
			p.cleanupIdleConnections()
		}
	}
}

// adaptationLoop runs the adaptive pooling algorithm
func (p *ConnectionPool) adaptationLoop() {
	if !p.config.AdaptivePoolingEnabled {
		return
	}
	
	ticker := time.NewTicker(p.config.AdaptationInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.done:
			return
		case <-ticker.C:
			p.adaptPoolSize()
		}
	}
}

// adaptPoolSize adjusts the pool size based on hit rate metrics
func (p *ConnectionPool) adaptPoolSize() {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Calculate current hit rate
	stats := p.stats
	totalRequests := stats.Hits + stats.Misses + stats.Timeouts
	if totalRequests == 0 {
		return
	}
	
	currentHitRate := float64(stats.Hits) / float64(totalRequests) * 100
	
	// Store hit rate history (keep last 10 samples)
	p.hitRateHistory = append(p.hitRateHistory, currentHitRate)
	if len(p.hitRateHistory) > 10 {
		p.hitRateHistory = p.hitRateHistory[1:]
	}
	
	// Calculate average hit rate
	var sum float64
	for _, rate := range p.hitRateHistory {
		sum += rate
	}
	avgHitRate := sum / float64(len(p.hitRateHistory))
	
	// Adjust pool size based on hit rate
	currentMax := p.config.MaxConnections
	minThreshold := p.config.MinHitRateThreshold
	maxThreshold := p.config.MaxHitRateThreshold
	
	if avgHitRate < minThreshold && currentMax < p.config.MaxAdaptiveConnections {
		// Expand pool
		newMax := int(float64(currentMax) * p.config.ExpansionFactor)
		if newMax > p.config.MaxAdaptiveConnections {
			newMax = p.config.MaxAdaptiveConnections
		}
		p.adjustPoolCapacity(newMax)
	} else if avgHitRate > maxThreshold && currentMax > p.config.MinAdaptiveConnections {
		// Contract pool
		newMax := int(float64(currentMax) * p.config.ContractionFactor)
		if newMax < p.config.MinAdaptiveConnections {
			newMax = p.config.MinAdaptiveConnections
		}
		p.adjustPoolCapacity(newMax)
	}
}

// adjustPoolCapacity changes the maximum pool capacity
func (p *ConnectionPool) adjustPoolCapacity(newMax int) {
	// This is a simplified implementation
	// In a production environment, you would need to handle
	// resizing the connections channel safely
	p.config.MaxConnections = newMax
}

// healthCheck performs health checks on connections
func (p *ConnectionPool) healthCheck() {
	atomic.AddUint64(&p.stats.HealthChecks, 1)
	// Implementation would depend on specific health check requirements
}

// GetHitRateHistory returns the recent hit rate history
func (p *ConnectionPool) GetHitRateHistory() []float64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	// Return a copy of the history
	history := make([]float64, len(p.hitRateHistory))
	copy(history, p.hitRateHistory)
	return history
}

// GetLastAdaptationTime returns the time of the last adaptation
func (p *ConnectionPool) GetLastAdaptationTime() time.Time {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.lastAdaptationTime
}

// SetAdaptiveConfig updates the adaptive pooling configuration
func (p *ConnectionPool) SetAdaptiveConfig(target, minThreshold, maxThreshold float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.config.TargetHitRate = target
	p.config.MinHitRateThreshold = minThreshold
	p.config.MaxHitRateThreshold = maxThreshold
}

// cleanupIdleConnections removes expired idle connections
func (p *ConnectionPool) cleanupIdleConnections() {
	// This is a simplified implementation
	// In a production environment, you would want to properly handle
	// connection removal without blocking other operations
}

// EnableAdaptivePooling enables or disables adaptive pooling
func (p *ConnectionPool) EnableAdaptivePooling(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	p.config.AdaptivePoolingEnabled = enabled
}

// ForceAdaptation triggers an immediate adaptation of the pool size
func (p *ConnectionPool) ForceAdaptation() {
	p.adaptPoolSize()
}