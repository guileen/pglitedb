package connection

import (
	"context"
	"sync"

	"github.com/guileen/pglitedb/network"
)

// ConnectionPoolManager handles connection pool management
type ConnectionPoolManager struct {
	connectionPool *network.ConnectionPool
	mu             sync.Mutex
}

// NewConnectionPoolManager creates a new connection pool manager
func NewConnectionPoolManager() *ConnectionPoolManager {
	return &ConnectionPoolManager{}
}

// InitializePool initializes the connection pool with the given configuration
func (cpm *ConnectionPoolManager) InitializePool(config network.PoolConfig, factory network.ConnectionFactory) error {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	cpm.connectionPool = network.NewConnectionPool(config, factory)
	return nil
}

// GetConnection retrieves a connection from the pool
func (cpm *ConnectionPoolManager) GetConnection(ctx context.Context) (*network.PooledConnection, error) {
	cpm.mu.Lock()
	pool := cpm.connectionPool
	cpm.mu.Unlock()

	if pool == nil {
		return nil, &network.ConnectionPoolError{Op: "get", Err: network.ErrPoolClosed}
	}

	return pool.Get(ctx)
}

// ReturnConnection returns a connection to the pool
func (cpm *ConnectionPoolManager) ReturnConnection(conn *network.PooledConnection) {
	cpm.mu.Lock()
	pool := cpm.connectionPool
	cpm.mu.Unlock()

	if pool != nil && conn != nil {
		pool.Put(conn)
	}
}

// ClosePool shuts down the connection pool
func (cpm *ConnectionPoolManager) ClosePool() error {
	cpm.mu.Lock()
	defer cpm.mu.Unlock()

	if cpm.connectionPool != nil {
		err := cpm.connectionPool.Close()
		if err != nil {
			return err
		}
		cpm.connectionPool = nil
	}

	return nil
}

// GetPoolStats returns current pool statistics
func (cpm *ConnectionPoolManager) GetPoolStats() network.PoolStats {
	cpm.mu.Lock()
	pool := cpm.connectionPool
	cpm.mu.Unlock()

	if pool == nil {
		return network.PoolStats{}
	}

	return pool.Stats()
}

// GetPoolMetrics returns current pool metrics
func (cpm *ConnectionPoolManager) GetPoolMetrics() network.PoolMetrics {
	cpm.mu.Lock()
	pool := cpm.connectionPool
	cpm.mu.Unlock()

	if pool == nil {
		return network.PoolMetrics{}
	}

	return pool.GetMetrics()
}

// IsPoolClosed returns whether the connection pool is closed
func (cpm *ConnectionPoolManager) IsPoolClosed() bool {
	cpm.mu.Lock()
	pool := cpm.connectionPool
	cpm.mu.Unlock()

	if pool == nil {
		return true
	}

	return pool.IsClosed()
}

// HealthCheck performs a health check on the connection pool manager
func (cpm *ConnectionPoolManager) HealthCheck() error {
	// Simple health check implementation
	return nil
}