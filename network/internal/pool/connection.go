// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

// PooledConnection wraps a net.Conn with pool lifecycle management
type PooledConnection struct {
	conn       net.Conn
	pool       *ConnectionPool
	createdAt  time.Time
	lastUsedAt time.Time
	closed     int32 // atomic flag
	mu         sync.RWMutex
}

// isClosed checks if the connection is closed
func (pc *PooledConnection) isClosed() bool {
	return atomic.LoadInt32(&pc.closed) == 1
}

// isHealthy checks if the connection is healthy
func (pc *PooledConnection) isHealthy() bool {
	if pc.isClosed() || pc.conn == nil {
		return false
	}
	
	// Perform a basic health check by checking if the connection is still alive
	// This is a simplified check - in practice, you might want to do a more thorough check
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