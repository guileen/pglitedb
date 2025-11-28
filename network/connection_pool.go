// Advanced connection pooling implementation for PostgreSQL connections with lifecycle management,
// health checking, metrics collection, and standardized error handling
package network

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
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

// ConnectionPoolError represents errors specific to connection pool operations
type ConnectionPoolError struct {
	Op  string
	Err error
}

func (e *ConnectionPoolError) Error() string {
	return fmt.Sprintf("connection pool error during %s: %v", e.Op, e.Err)
}

func (e *ConnectionPoolError) Unwrap() error {
	return e.Err
}

// IsConnectionPoolError checks if an error is a connection pool error
func IsConnectionPoolError(err error) bool {
	var target *ConnectionPoolError
	return errors.As(err, &target)
}

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

// PooledConnection wraps a net.Conn with pool lifecycle management
type PooledConnection struct {
	conn       net.Conn
	pool       *ConnectionPool
	createdAt  time.Time
	lastUsedAt time.Time
	closed     int32 // atomic flag
	mu         sync.RWMutex
	
	// Health tracking
	lastHealthCheck time.Time
	healthFailures  int32
}

// ConnectionFactory interface for creating new connections
type ConnectionFactory interface {
	CreateConnection(ctx context.Context) (net.Conn, error)
}

// NewConnectionPool creates a new connection pool with advanced lifecycle management
func NewConnectionPool(config PoolConfig, factory ConnectionFactory) *ConnectionPool {
	if config.MaxConnections <= 0 {
		config.MaxConnections = 10
	}
	if config.MinConnections < 0 {
		config.MinConnections = 0
	}
	if config.MinConnections > config.MaxConnections {
		config.MinConnections = config.MaxConnections
	}
	if config.IdleTimeout <= 0 {
		config.IdleTimeout = 30 * time.Minute
	}
	if config.MaxLifetime <= 0 {
		config.MaxLifetime = 1 * time.Hour
	}
	if config.ConnectionTimeout <= 0 {
		config.ConnectionTimeout = 30 * time.Second
	}
	if config.MaxIdleConns <= 0 {
		config.MaxIdleConns = config.MaxConnections
	}
	if config.HealthCheckPeriod <= 0 {
		config.HealthCheckPeriod = 1 * time.Minute
	}
	
	// Set adaptive pooling defaults
	if config.TargetHitRate <= 0 {
		config.TargetHitRate = 80.0 // 80% target hit rate
	}
	if config.MinHitRateThreshold <= 0 {
		config.MinHitRateThreshold = 50.0 // Expand if hit rate drops below 50%
	}
	if config.MaxHitRateThreshold <= 0 {
		config.MaxHitRateThreshold = 95.0 // Contract if hit rate exceeds 95%
	}
	if config.AdaptationInterval <= 0 {
		config.AdaptationInterval = 5 * time.Minute // Adapt every 5 minutes
	}
	if config.ExpansionFactor <= 1.0 {
		config.ExpansionFactor = 1.5 // Expand by 50%
	}
	if config.ContractionFactor <= 0 || config.ContractionFactor >= 1.0 {
		config.ContractionFactor = 0.8 // Contract by 20%
	}
	if config.MaxAdaptiveConnections <= 0 {
		config.MaxAdaptiveConnections = config.MaxConnections * 2 // Double the max connections
	}
	if config.MinAdaptiveConnections <= 0 {
		config.MinAdaptiveConnections = config.MinConnections
	}

	pool := &ConnectionPool{
		config:             config,
		factory:            factory,
		connections:        make(chan *PooledConnection, config.MaxAdaptiveConnections),
		done:               make(chan struct{}),
		lastAdaptationTime: time.Now(),
		hitRateHistory:     make([]float64, 0, 10), // Keep last 10 hit rate measurements
	}

	// Pre-create minimum connections
	for i := 0; i < config.MinConnections; i++ {
		go pool.createConnection()
	}

	// Start background maintenance
	go pool.maintenance()
	
	// Start adaptive pooling if enabled
	if config.AdaptivePoolingEnabled {
		go pool.adaptationLoop()
	}

	return pool
}

// createConnection creates a new connection and adds it to the pool if there's space
func (p *ConnectionPool) createConnection() {
	// Check if we're allowed to create more connections
	currentActive := atomic.LoadInt32(&p.activeCount)
	if atomic.LoadInt32(&p.closed) == 1 || currentActive >= int32(p.config.MaxConnections) {
		return
	}

	// Atomically increment active count
	if !atomic.CompareAndSwapInt32(&p.activeCount, currentActive, currentActive+1) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.config.ConnectionTimeout)
	defer cancel()

	conn, err := p.factory.CreateConnection(ctx)
	if err != nil {
		atomic.AddInt32(&p.activeCount, -1)
		atomic.AddUint64(&p.stats.ConnectionErrors, 1)
		return
	}

	pooledConn := &PooledConnection{
		conn:       conn,
		pool:       p,
		createdAt:  time.Now(),
		lastUsedAt: time.Now(),
	}

	// Try to add to pool, if pool is full, close the connection
	select {
	case p.connections <- pooledConn:
		// Successfully added to pool
		atomic.AddUint64(&p.stats.TotalConns, 1)
		atomic.AddUint64(&p.stats.IdleConns, 1)
	default:
		// Pool is full, close connection
		pooledConn.forceClose()
		atomic.AddInt32(&p.activeCount, -1)
	}
}

// Get acquires a connection from the pool with advanced timeout handling
func (p *ConnectionPool) Get(ctx context.Context) (*PooledConnection, error) {
	// Fast path: try to get a connection immediately
	select {
	case conn := <-p.connections:
		atomic.AddUint64(&p.stats.Hits, 1)
		atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
		atomic.AddUint64(&p.stats.ActiveConns, 1)
		conn.updateLastUsed()
		return conn, nil
	default:
		// No available connections immediately
	}

	// Check if we can create a new connection
	currentActive := atomic.LoadInt32(&p.activeCount)
	if currentActive < int32(p.config.MaxConnections) {
		// Try to atomically increment active count
		if atomic.CompareAndSwapInt32(&p.activeCount, currentActive, currentActive+1) {
			conn, err := p.factory.CreateConnection(ctx)
			if err != nil {
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.Misses, 1)
				atomic.AddUint64(&p.stats.ConnectionErrors, 1)
				return nil, &ConnectionPoolError{
					Op:  "create_connection",
					Err: err,
				}
			}

			atomic.AddUint64(&p.stats.Misses, 1)
			atomic.AddUint64(&p.stats.TotalConns, 1)
			atomic.AddUint64(&p.stats.ActiveConns, 1)

			return &PooledConnection{
				conn:       conn,
				pool:       p,
				createdAt:  time.Now(),
				lastUsedAt: time.Now(),
			}, nil
		}
	}

	// At max connections, wait for one to become available with context awareness
	select {
	case conn := <-p.connections:
		atomic.AddUint64(&p.stats.Hits, 1)
		atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
		atomic.AddUint64(&p.stats.ActiveConns, 1)
		conn.updateLastUsed()
		return conn, nil
	case <-ctx.Done():
		atomic.AddUint64(&p.stats.Timeouts, 1)
		return nil, &ConnectionPoolError{
			Op:  "acquire_connection",
			Err: ctx.Err(),
		}
	}
}

// Put returns a connection to the pool with proper lifecycle management
func (p *ConnectionPool) Put(conn *PooledConnection) {
	if conn.isClosed() {
		return
	}

	// Check if connection is healthy and not expired
	if !conn.isHealthy() || conn.isExpired(p.config.MaxLifetime) {
		conn.forceClose()
		atomic.AddInt32(&p.activeCount, -1)
		atomic.AddUint64(&p.stats.ActiveConns, ^uint64(0)) // decrement
		atomic.AddUint64(&p.stats.ClosedConns, 1)
		if !conn.isHealthy() {
			atomic.AddUint64(&p.stats.FailedHealth, 1)
		}
		// Try to create a replacement connection asynchronously
		go p.createConnection()
		return
	}

	// Check if we should keep this connection in the pool
	idleConns := atomic.LoadUint64(&p.stats.IdleConns)
	if idleConns >= uint64(p.config.MaxIdleConns) {
		// Pool is full, close the connection
		conn.forceClose()
		atomic.AddInt32(&p.activeCount, -1)
		atomic.AddUint64(&p.stats.ActiveConns, ^uint64(0)) // decrement
		atomic.AddUint64(&p.stats.ClosedConns, 1)
		return
	}

	// Return connection to pool
	select {
	case p.connections <- conn:
		// Connection returned to pool
		atomic.AddUint64(&p.stats.IdleConns, 1)
		atomic.AddUint64(&p.stats.ActiveConns, ^uint64(0)) // decrement
	default:
		// Pool is full, close the connection
		conn.forceClose()
		atomic.AddInt32(&p.activeCount, -1)
		atomic.AddUint64(&p.stats.ActiveConns, ^uint64(0)) // decrement
		atomic.AddUint64(&p.stats.ClosedConns, 1)
	}
}

// Close shuts down the connection pool gracefully
func (p *ConnectionPool) Close() error {
	// Atomically set closed flag
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return nil // Already closed
	}

	close(p.done) // Signal maintenance to stop

	// Close all pooled connections
	var firstErr error
	for {
		select {
		case conn := <-p.connections:
			if err := conn.forceClose(); err != nil && firstErr == nil {
				firstErr = err
			}
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
			atomic.AddInt32(&p.activeCount, -1)
		default:
			// No more connections in the channel
			return firstErr
		}
	}
}

// Stats returns current pool statistics
func (p *ConnectionPool) Stats() PoolStats {
	return PoolStats{
		Hits:            atomic.LoadUint64(&p.stats.Hits),
		Misses:          atomic.LoadUint64(&p.stats.Misses),
		Timeouts:        atomic.LoadUint64(&p.stats.Timeouts),
		TotalConns:      atomic.LoadUint64(&p.stats.TotalConns),
		IdleConns:       atomic.LoadUint64(&p.stats.IdleConns),
		ActiveConns:     atomic.LoadUint64(&p.stats.ActiveConns),
		HealthChecks:    atomic.LoadUint64(&p.stats.HealthChecks),
		FailedHealth:    atomic.LoadUint64(&p.stats.FailedHealth),
		ConnectionErrors: atomic.LoadUint64(&p.stats.ConnectionErrors),
		ClosedConns:     atomic.LoadUint64(&p.stats.ClosedConns),
	}
}

// PoolMetrics represents formatted metrics for reporting
type PoolMetrics struct {
	// Basic pool statistics
	CurrentConnections int64
	ActiveConnections  int64
	IdleConnections    int64
	TotalConnections   uint64
	
	// Operation metrics
	ConnectionHits     uint64
	ConnectionMisses   uint64
	ConnectionTimeouts uint64
	ConnectionErrors   uint64
	
	// Health metrics
	HealthChecks     uint64
	FailedHealth     uint64
	ClosedConnections uint64
	
	// Rates (per second)
	HitRate     float64
	ErrorRate   float64
	HealthRate  float64
}

// GetMetrics returns formatted pool metrics for reporting
func (p *ConnectionPool) GetMetrics() PoolMetrics {
	stats := p.Stats()
	
	// Calculate rates
	totalOps := float64(stats.Hits + stats.Misses + stats.Timeouts)
	hitRate := 0.0
	errorRate := 0.0
	healthRate := 0.0
	
	if totalOps > 0 {
		hitRate = float64(stats.Hits) / totalOps * 100
	}
	
	totalConnections := float64(stats.TotalConns)
	if totalConnections > 0 {
		errorRate = float64(stats.ConnectionErrors) / totalConnections * 100
		if stats.HealthChecks > 0 {
			healthRate = float64(stats.FailedHealth) / float64(stats.HealthChecks) * 100
		}
	}
	
	return PoolMetrics{
		CurrentConnections: int64(atomic.LoadInt32(&p.activeCount)),
		ActiveConnections:  int64(stats.ActiveConns),
		IdleConnections:    int64(stats.IdleConns),
		TotalConnections:   stats.TotalConns,
		
		ConnectionHits:     stats.Hits,
		ConnectionMisses:   stats.Misses,
		ConnectionTimeouts: stats.Timeouts,
		ConnectionErrors:   stats.ConnectionErrors,
		
		HealthChecks:      stats.HealthChecks,
		FailedHealth:      stats.FailedHealth,
		ClosedConnections: stats.ClosedConns,
		
		HitRate:    hitRate,
		ErrorRate:  errorRate,
		HealthRate: healthRate,
	}
}

// IsAdaptivePoolingEnabled returns whether adaptive pooling is enabled
func (p *ConnectionPool) IsAdaptivePoolingEnabled() bool {
	return p.config.AdaptivePoolingEnabled
}

// GetAdaptiveConfig returns the adaptive pooling configuration
func (p *ConnectionPool) GetAdaptiveConfig() (target, minThreshold, maxThreshold float64) {
	return p.config.TargetHitRate, p.config.MinHitRateThreshold, p.config.MaxHitRateThreshold
}

// GetPoolCapacity returns the current and maximum pool capacities
func (p *ConnectionPool) GetPoolCapacity() (current, max int) {
	return int(atomic.LoadInt32(&p.activeCount)), p.config.MaxConnections
}

// maintenance performs periodic cleanup of idle and expired connections
func (p *ConnectionPool) maintenance() {
	healthTicker := time.NewTicker(p.config.HealthCheckPeriod)
	idleTicker := time.NewTicker(p.config.IdleTimeout / 2) // Check idle connections more frequently
	defer healthTicker.Stop()
	defer idleTicker.Stop()

	for {
		select {
		case <-healthTicker.C:
			p.healthCheck()
		case <-idleTicker.C:
			p.cleanupIdleConnections()
		case <-p.done:
			return
		}
	}
}

// adaptationLoop performs periodic adaptation of pool size based on usage patterns
func (p *ConnectionPool) adaptationLoop() {
	if !p.config.AdaptivePoolingEnabled {
		return
	}
	
	adaptTicker := time.NewTicker(p.config.AdaptationInterval)
	defer adaptTicker.Stop()

	for {
		select {
		case <-adaptTicker.C:
			p.adaptPoolSize()
		case <-p.done:
			return
		}
	}
}

// adaptPoolSize adjusts the pool size based on hit rate metrics and usage patterns
func (p *ConnectionPool) adaptPoolSize() {
	if atomic.LoadInt32(&p.closed) == 1 {
		return
	}
	
	// Calculate current hit rate
	metrics := p.GetMetrics()
	currentHitRate := metrics.HitRate
	
	// Store hit rate in history for trend analysis
	p.mu.Lock()
	p.hitRateHistory = append(p.hitRateHistory, currentHitRate)
	if len(p.hitRateHistory) > 10 {
		p.hitRateHistory = p.hitRateHistory[1:] // Keep only last 10 measurements
	}
	p.mu.Unlock()
	
	// Determine if we need to adapt based on hit rate trends
	shouldExpand := currentHitRate < p.config.MinHitRateThreshold
	shouldContract := currentHitRate > p.config.MaxHitRateThreshold
	
	// Get current pool configuration
	currentActive := int(atomic.LoadInt32(&p.activeCount))
	currentMax := p.config.MaxConnections
	targetMax := currentMax
	
	if shouldExpand {
		// Expand pool size
		newMax := int(float64(currentMax) * p.config.ExpansionFactor)
		if newMax > p.config.MaxAdaptiveConnections {
			newMax = p.config.MaxAdaptiveConnections
		}
		if newMax > currentMax {
			targetMax = newMax
		}
	} else if shouldContract {
		// Contract pool size only if we're above min connections
		if currentMax > p.config.MinAdaptiveConnections {
			newMax := int(float64(currentMax) * p.config.ContractionFactor)
			if newMax < p.config.MinAdaptiveConnections {
				newMax = p.config.MinAdaptiveConnections
			}
			// Only contract if we're not using most of the connections
			idleConns := int(atomic.LoadUint64(&p.stats.IdleConns))
			if newMax < currentMax && (currentActive-idleConns) < newMax {
				targetMax = newMax
			}
		}
	}
	
	// Update pool size if needed
	if targetMax != currentMax {
		p.adjustPoolCapacity(targetMax)
	}
	
	// Update last adaptation time
	p.mu.Lock()
	p.lastAdaptationTime = time.Now()
	p.mu.Unlock()
}

// adjustPoolCapacity changes the maximum capacity of the connection pool
func (p *ConnectionPool) adjustPoolCapacity(newMax int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	
	if newMax <= 0 || newMax == p.config.MaxConnections {
		return
	}
	
	// Create new channel with adjusted capacity
	oldConnections := p.connections
	newConnections := make(chan *PooledConnection, newMax)
	
	// Transfer existing connections to new channel
	close(oldConnections) // Close old channel to prevent further writes
	transferred := 0
	
	for conn := range oldConnections {
		if transferred < newMax {
			select {
			case newConnections <- conn:
				transferred++
			default:
				// New channel is full, close extra connections
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
				atomic.AddUint64(&p.stats.ClosedConns, 1)
			}
		} else {
			// Close excess connections
			conn.forceClose()
			atomic.AddInt32(&p.activeCount, -1)
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
			atomic.AddUint64(&p.stats.ClosedConns, 1)
		}
	}
	
	// Update pool configuration and channel
	p.config.MaxConnections = newMax
	p.connections = newConnections
	
	// If we expanded and are below the new max, create new connections
	currentActive := int(atomic.LoadInt32(&p.activeCount))
	if newMax > currentActive {
		connectionsToCreate := newMax - currentActive
		if connectionsToCreate > 5 { // Limit burst creation
			connectionsToCreate = 5
		}
		for i := 0; i < connectionsToCreate; i++ {
			go p.createConnection()
		}
	}
}

// healthCheck performs periodic health checks on pooled connections
func (p *ConnectionPool) healthCheck() {
	atomic.AddUint64(&p.stats.HealthChecks, 1)
	
	// Create a temporary slice to hold connections during health check
	var conns []*PooledConnection
	defer func() {
		// Return all connections back to the pool
		for _, conn := range conns {
			select {
			case p.connections <- conn:
			default:
				// Pool is full, close connection
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.ClosedConns, 1)
			}
		}
	}()

	// Drain the pool temporarily for health checking
	drained := 0
	for drained < p.config.MaxConnections {
		select {
		case conn := <-p.connections:
			conns = append(conns, conn)
			drained++
		default:
			break
		}
	}

	// Health check each connection
	healthyCount := 0
	for _, conn := range conns {
		if !conn.isHealthy() || conn.isExpired(p.config.MaxLifetime) {
			// Remove unhealthy connection
			conn.forceClose()
			atomic.AddInt32(&p.activeCount, -1)
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
			atomic.AddUint64(&p.stats.FailedHealth, 1)
			atomic.AddUint64(&p.stats.ClosedConns, 1)
			// Try to create a replacement connection asynchronously
			go p.createConnection()
		} else {
			healthyCount++
			// Keep healthy connection in the pool
			select {
			case p.connections <- conn:
			default:
				// Pool is full, close connection
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
				atomic.AddUint64(&p.stats.ClosedConns, 1)
			}
		}
	}
}

// cleanupIdleConnections removes connections that have been idle too long
func (p *ConnectionPool) cleanupIdleConnections() {
	now := time.Now()
	idleTimeout := p.config.IdleTimeout
	
	// Create a temporary slice to hold connections
	var conns []*PooledConnection
	defer func() {
		// Return connections back to the pool
		for _, conn := range conns {
			select {
			case p.connections <- conn:
			default:
				// Pool is full, close connection
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
				atomic.AddUint64(&p.stats.ClosedConns, 1)
			}
		}
	}()

	// Drain the pool temporarily
	drained := 0
	for drained < p.config.MaxConnections {
		select {
		case conn := <-p.connections:
			conns = append(conns, conn)
			drained++
		default:
			break
		}
	}

	// Check each connection for idle timeout
	closedCount := 0
	for _, conn := range conns {
		conn.mu.RLock()
		idleTime := now.Sub(conn.lastUsedAt)
		conn.mu.RUnlock()

		if idleTime > idleTimeout {
			// Close idle connection
			conn.forceClose()
			atomic.AddInt32(&p.activeCount, -1)
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
			atomic.AddUint64(&p.stats.ClosedConns, 1)
			closedCount++
			// Try to create a replacement connection if we're below min connections
			currentActive := atomic.LoadInt32(&p.activeCount)
			if int(currentActive) < p.config.MinConnections {
				go p.createConnection()
			}
		} else {
			// Return connection to pool
			select {
			case p.connections <- conn:
			default:
				// Pool is full, close connection
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
				atomic.AddUint64(&p.stats.ClosedConns, 1)
				closedCount++
			}
		}
	}
}

// isClosed checks if the connection is closed
func (pc *PooledConnection) isClosed() bool {
	return atomic.LoadInt32(&pc.closed) == 1
}

// isHealthy checks if the connection is still healthy with enhanced error handling
func (pc *PooledConnection) isHealthy() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	
	// Skip health check if recently checked (within 100ms)
	if time.Since(pc.lastHealthCheck) < 100*time.Millisecond {
		return atomic.LoadInt32(&pc.healthFailures) == 0
	}
	
	// Update last health check time
	pc.lastHealthCheck = time.Now()
	
	// Try a non-blocking read with a short timeout to check connection health
	_ = pc.conn.SetReadDeadline(time.Now().Add(1 * time.Millisecond))
	var buf [1]byte
	_, err := pc.conn.Read(buf[:])
	
	// Reset deadline
	_ = pc.conn.SetReadDeadline(time.Time{})
	
	// If we got EOF or a timeout, connection is likely closed
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			// Timeout is expected for healthy connection with no data
			atomic.StoreInt32(&pc.healthFailures, 0)
			return true
		}
		if errors.Is(err, net.ErrClosed) {
			atomic.AddInt32(&pc.healthFailures, 1)
			return false
		}
		// Other errors might indicate connection problems
		isHealthy := err == nil || err.Error() == "EOF"
		if !isHealthy {
			atomic.AddInt32(&pc.healthFailures, 1)
		} else {
			atomic.StoreInt32(&pc.healthFailures, 0)
		}
		return isHealthy
	}
	
	atomic.StoreInt32(&pc.healthFailures, 0)
	return true
}

// isExpired checks if the connection has exceeded its maximum lifetime
func (pc *PooledConnection) isExpired(maxLifetime time.Duration) bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	return time.Since(pc.createdAt) > maxLifetime
}

// updateLastUsed updates the last used timestamp
func (pc *PooledConnection) updateLastUsed() {
	pc.mu.Lock()
	defer pc.mu.Unlock()
	pc.lastUsedAt = time.Now()
}

// forceClose closes the underlying connection without pool management
func (pc *PooledConnection) forceClose() error {
	if !atomic.CompareAndSwapInt32(&pc.closed, 0, 1) {
		return nil // Already closed
	}
	
	pc.mu.Lock()
	conn := pc.conn
	pc.mu.Unlock()
	
	if conn != nil {
		return conn.Close()
	}
	return nil
}

// Close closes the connection and returns it to the pool
func (pc *PooledConnection) Close() error {
	if !atomic.CompareAndSwapInt32(&pc.closed, 0, 1) {
		return nil // Already closed
	}

	// Return to pool if it exists and pool is not closed
	if pc.pool != nil && atomic.LoadInt32(&pc.pool.closed) == 0 {
		// Reset closed flag since we're returning to pool
		atomic.StoreInt32(&pc.closed, 0)
		pc.pool.Put(pc)
		return nil
	}

	// Otherwise close directly
	return pc.conn.Close()
}

// Read implements io.Reader
func (pc *PooledConnection) Read(b []byte) (n int, err error) {
	if pc.isClosed() {
		return 0, &net.OpError{Op: "read", Err: &net.AddrError{Err: "connection closed"}}
	}
	return pc.conn.Read(b)
}

// Write implements io.Writer
func (pc *PooledConnection) Write(b []byte) (n int, err error) {
	if pc.isClosed() {
		return 0, &net.OpError{Op: "write", Err: &net.AddrError{Err: "connection closed"}}
	}
	return pc.conn.Write(b)
}

// LocalAddr returns the local network address
func (pc *PooledConnection) LocalAddr() net.Addr {
	return pc.conn.LocalAddr()
}

// RemoteAddr returns the remote network address
func (pc *PooledConnection) RemoteAddr() net.Addr {
	return pc.conn.RemoteAddr()
}

// SetDeadline sets the read and write deadlines
func (pc *PooledConnection) SetDeadline(t time.Time) error {
	if pc.isClosed() {
		return &net.OpError{Op: "set", Err: &net.AddrError{Err: "connection closed"}}
	}
	return pc.conn.SetDeadline(t)
}

// SetReadDeadline sets the read deadline
func (pc *PooledConnection) SetReadDeadline(t time.Time) error {
	if pc.isClosed() {
		return &net.OpError{Op: "set", Err: &net.AddrError{Err: "connection closed"}}
	}
	return pc.conn.SetReadDeadline(t)
}

// SetWriteDeadline sets the write deadline
func (pc *PooledConnection) SetWriteDeadline(t time.Time) error {
	if pc.isClosed() {
		return &net.OpError{Op: "set", Err: &net.AddrError{Err: "connection closed"}}
	}
	return pc.conn.SetWriteDeadline(t)
}