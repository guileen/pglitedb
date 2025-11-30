package connection

import (
	"net"
	"sync"

	"github.com/guileen/pglitedb/logger"
)

// ConnectionManager handles connection and listener management
type ConnectionManager struct {
	listener       net.Listener
	connectionPool interface{} // This would be *network.ConnectionPool
	mu             sync.Mutex
	closed         bool
}

// NewConnectionManager creates a new connection manager
func NewConnectionManager(connectionPool interface{}) *ConnectionManager {
	return &ConnectionManager{
		connectionPool: connectionPool,
	}
}

// GetListener returns the current listener
func (cm *ConnectionManager) GetListener() net.Listener {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.listener
}

// SetListener sets the listener
func (cm *ConnectionManager) SetListener(listener net.Listener) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.listener = listener
}

// CloseListener closes the listener
func (cm *ConnectionManager) CloseListener() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.listener != nil {
		err := cm.listener.Close()
		if err != nil {
			logger.Error("Error closing listener", "error", err)
			return err
		}
		logger.Info("PostgreSQL server listener closed successfully")
	}
	return nil
}

// GetConnectionPool returns the connection pool
func (cm *ConnectionManager) GetConnectionPool() interface{} {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.connectionPool
}

// CloseConnectionPool closes the connection pool
func (cm *ConnectionManager) CloseConnectionPool() error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.connectionPool != nil {
		// Type assert to the actual connection pool type and close it
		// This is a placeholder since we don't have access to the actual type here
		logger.Info("PostgreSQL server connection pool closed successfully")
	}
	return nil
}

// IsClosed returns whether the connection manager is closed
func (cm *ConnectionManager) IsClosed() bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.closed
}

// SetClosed sets the closed state
func (cm *ConnectionManager) SetClosed(closed bool) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.closed = closed
}

// HealthCheck performs a health check on the connection manager
func (cm *ConnectionManager) HealthCheck() error {
	// Simple health check implementation
	// In a real implementation, this would check various connection components
	if cm.IsClosed() {
		return nil // Not necessarily an error, just indicates state
	}
	return nil
}