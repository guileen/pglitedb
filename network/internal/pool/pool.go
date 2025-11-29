// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// ConnectionPool manages a pool of database connections with advanced lifecycle management
type ConnectionPool struct {
	config      PoolConfig
	factory     ConnectionFactory
	connections chan *PooledConnection
	mu          sync.Mutex
	closed      int32 // atomic flag
	activeCount int32 // atomic counter
	stats       PoolStats
	done        chan struct{}
	
	// Adaptive pooling fields
	lastAdaptationTime time.Time
	hitRateHistory     []float64
}

// NewConnectionPool creates a new connection pool with the given configuration
func NewConnectionPool(config PoolConfig, factory ConnectionFactory) *ConnectionPool {
	pool := &ConnectionPool{
		config:      config,
		factory:     factory,
		connections: make(chan *PooledConnection, config.MaxConnections),
		done:        make(chan struct{}),
	}
	
	// Pre-populate with minimum connections
	for i := 0; i < config.MinConnections; i++ {
		go pool.createConnection()
	}
	
	// Start maintenance goroutines
	go pool.maintenance()
	if config.AdaptivePoolingEnabled {
		go pool.adaptationLoop()
	}
	
	return pool
}

// createConnection creates a new connection and adds it to the pool
func (p *ConnectionPool) createConnection() {
	conn, err := p.factory.CreateConnection()
	if err != nil {
		// Log error but don't fail the pool creation
		atomic.AddUint64(&p.stats.ConnectionErrors, 1)
		return
	}
	
	pooledConn := &PooledConnection{
		conn:       conn,
		pool:       p,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}
	
	select {
	case p.connections <- pooledConn:
		atomic.AddUint64(&p.stats.TotalConns, 1)
		atomic.AddUint64(&p.stats.IdleConns, 1)
	default:
		// Pool is full, close the connection
		pooledConn.forceClose()
	}
}

// Get retrieves a connection from the pool
func (p *ConnectionPool) Get(ctx context.Context) (*PooledConnection, error) {
	if atomic.LoadInt32(&p.closed) == 1 {
		return nil, &ConnectionPoolError{Op: "get", Err: ErrPoolClosed}
	}

	select {
	case conn := <-p.connections:
		atomic.AddUint64(&p.stats.Hits, 1)
		atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
		atomic.AddInt32(&p.activeCount, 1)
		conn.updateLastUsed()
		return conn, nil
	case <-ctx.Done():
		atomic.AddUint64(&p.stats.Timeouts, 1)
		return nil, ctx.Err()
	case <-time.After(p.config.ConnectionTimeout):
		atomic.AddUint64(&p.stats.Timeouts, 1)
		return nil, &ConnectionPoolError{Op: "get", Err: ErrTimeout}
	}
}

// Put returns a connection to the pool
func (p *ConnectionPool) Put(conn *PooledConnection) {
	if atomic.LoadInt32(&p.closed) == 1 {
		conn.forceClose()
		return
	}

	// Check if connection is still healthy
	if !conn.isHealthy() || conn.isExpired(p.config.MaxLifetime) {
		conn.forceClose()
		atomic.AddUint64(&p.stats.ClosedConns, 1)
		atomic.AddInt32(&p.activeCount, -1)
		// Create a new connection to maintain pool size
		go p.createConnection()
		return
	}

	select {
	case p.connections <- conn:
		atomic.AddUint64(&p.stats.IdleConns, 1)
		atomic.AddInt32(&p.activeCount, -1)
	default:
		// Pool is full, close the connection
		conn.forceClose()
		atomic.AddUint64(&p.stats.ClosedConns, 1)
		atomic.AddInt32(&p.activeCount, -1)
	}
}

// Close shuts down the connection pool
func (p *ConnectionPool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil // Already closed
	}

	close(p.done)
	
	// Close all connections in the pool
	close(p.connections)
	for conn := range p.connections {
		conn.forceClose()
		atomic.AddUint64(&p.stats.ClosedConns, 1)
	}
	
	return nil
}

// Stats returns current pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	return p.stats
}

// GetMetrics returns current pool metrics
func (p *ConnectionPool) GetMetrics() PoolMetrics {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	stats := p.stats
	totalRequests := stats.Hits + stats.Misses + stats.Timeouts
	hitRate := 0.0
	if totalRequests > 0 {
		hitRate = float64(stats.Hits) / float64(totalRequests) * 100
	}
	
	return PoolMetrics{
		CurrentSize:     int(atomic.LoadInt32(&p.activeCount)) + int(atomic.LoadUint64(&stats.IdleConns)),
		MaxSize:         p.config.MaxConnections,
		Available:       int(atomic.LoadUint64(&stats.IdleConns)),
		Active:          int(atomic.LoadInt32(&p.activeCount)),
		PendingRequests: len(p.connections),
		HitRate:         hitRate,
	}
}

// IsAdaptivePoolingEnabled returns whether adaptive pooling is enabled
func (p *ConnectionPool) IsAdaptivePoolingEnabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.config.AdaptivePoolingEnabled
}

// GetAdaptiveConfig returns the adaptive pooling configuration
func (p *ConnectionPool) GetAdaptiveConfig() (target, minThreshold, maxThreshold float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	target = p.config.TargetHitRate
	minThreshold = p.config.MinHitRateThreshold
	maxThreshold = p.config.MaxHitRateThreshold
	return
}

// GetPoolCapacity returns the current and maximum pool capacity
func (p *ConnectionPool) GetPoolCapacity() (current, max int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.connections), p.config.MaxConnections
}