package network

import (
	"net"
	"sync"
	"time"
)

// PoolConfig defines configuration for the connection pool
type PoolConfig struct {
	MaxConnections    int
	MinConnections    int
	ConnectionTimeout time.Duration
	IdleTimeout       time.Duration
	MaxLifetime       time.Duration
	MaxIdleConns      int
	HealthCheckPeriod time.Duration
	
	// Metrics configuration
	MetricsEnabled bool
	
	// Adaptive pooling configuration
	AdaptivePoolingEnabled bool
	TargetHitRate          float64 // Target hit rate percentage (0-100)
	MinHitRateThreshold    float64 // Minimum hit rate threshold to trigger expansion (0-100)
	MaxHitRateThreshold    float64 // Maximum hit rate threshold to trigger contraction (0-100)
	AdaptationInterval     time.Duration // How often to check and adapt pool size
	ExpansionFactor        float64 // Factor by which to expand pool (e.g., 1.5 = 50% increase)
	ContractionFactor      float64 // Factor by which to contract pool (e.g., 0.8 = 20% decrease)
	MaxAdaptiveConnections int     // Maximum connections when using adaptive sizing
	MinAdaptiveConnections int     // Minimum connections when using adaptive sizing
}

// PoolStats contains statistics about the pool
type PoolStats struct {
	Hits        uint64 // number of times a connection was found in the pool
	Misses      uint64 // number of times a new connection was created
	Timeouts    uint64 // number of times a connection request timed out
	TotalConns  uint64 // total number of connections created
	IdleConns   uint64 // current number of idle connections
	ActiveConns uint64 // current number of active connections
	
	// Health and error metrics
	HealthChecks    uint64 // number of health checks performed
	FailedHealth    uint64 // number of failed health checks
	ConnectionErrors uint64 // number of connection errors
	ClosedConns     uint64 // number of closed connections
}

// PoolMetrics contains runtime metrics for the pool
type PoolMetrics struct {
	CurrentSize     int
	MaxSize         int
	Available       int
	Active          int
	PendingRequests int
	HitRate         float64
	ErrorRate       float64
	AvgWaitTime     time.Duration
}

// PooledConnection wraps a net.Conn with pool lifecycle management
type PooledConnection struct {
	conn       net.Conn
	pool       *ConnectionPool
	createdAt  time.Time
	lastUsedAt time.Time
	closed     int32 // atomic flag
	mu         sync.RWMutex
}