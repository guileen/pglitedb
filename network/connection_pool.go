// Advanced connection pooling implementation for PostgreSQL connections with lifecycle management
package network

import (
	"context"
	"errors"
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
}

// PoolStats contains statistics about the pool
type PoolStats struct {
	Hits        uint64 // number of times a connection was found in the pool
	Misses      uint64 // number of times a new connection was created
	Timeouts    uint64 // number of times a connection request timed out
	TotalConns  uint64 // total number of connections created
	IdleConns   uint64 // current number of idle connections
	ActiveConns uint64 // current number of active connections
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

	pool := &ConnectionPool{
		config:      config,
		factory:     factory,
		connections: make(chan *PooledConnection, config.MaxConnections),
		done:        make(chan struct{}),
	}

	// Pre-create minimum connections
	for i := 0; i < config.MinConnections; i++ {
		go pool.createConnection()
	}

	// Start background maintenance
	go pool.maintenance()

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
				return nil, err
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
		return nil, ctx.Err()
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
		Hits:        atomic.LoadUint64(&p.stats.Hits),
		Misses:      atomic.LoadUint64(&p.stats.Misses),
		Timeouts:    atomic.LoadUint64(&p.stats.Timeouts),
		TotalConns:  atomic.LoadUint64(&p.stats.TotalConns),
		IdleConns:   atomic.LoadUint64(&p.stats.IdleConns),
		ActiveConns: atomic.LoadUint64(&p.stats.ActiveConns),
	}
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

// healthCheck performs periodic health checks on pooled connections
func (p *ConnectionPool) healthCheck() {
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
	for _, conn := range conns {
		if !conn.isHealthy() || conn.isExpired(p.config.MaxLifetime) {
			// Remove unhealthy connection
			conn.forceClose()
			atomic.AddInt32(&p.activeCount, -1)
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
			// Try to create a replacement connection asynchronously
			go p.createConnection()
		} else {
			// Keep healthy connection in the pool
			select {
			case p.connections <- conn:
			default:
				// Pool is full, close connection
				conn.forceClose()
				atomic.AddInt32(&p.activeCount, -1)
				atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
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
	for _, conn := range conns {
		conn.mu.RLock()
		idleTime := now.Sub(conn.lastUsedAt)
		conn.mu.RUnlock()

		if idleTime > idleTimeout {
			// Close idle connection
			conn.forceClose()
			atomic.AddInt32(&p.activeCount, -1)
			atomic.AddUint64(&p.stats.IdleConns, ^uint64(0)) // decrement
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
			}
		}
	}
}

// isClosed checks if the connection is closed
func (pc *PooledConnection) isClosed() bool {
	return atomic.LoadInt32(&pc.closed) == 1
}

// isHealthy checks if the connection is still healthy
func (pc *PooledConnection) isHealthy() bool {
	pc.mu.RLock()
	defer pc.mu.RUnlock()
	
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
			return true
		}
		if errors.Is(err, net.ErrClosed) || errors.Is(err, net.ErrClosed) {
			return false
		}
		// Other errors might indicate connection problems
		return err == nil || err.Error() == "EOF"
	}
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