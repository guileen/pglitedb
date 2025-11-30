package interfaces

import (
	"context"
	
	"github.com/guileen/pglitedb/network"
)

// ConnectionPoolManagementInterface defines the interface for connection pool management operations
type ConnectionPoolManagementInterface interface {
	InitializePool(config network.PoolConfig, factory network.ConnectionFactory) error
	GetConnection(ctx context.Context) (*network.PooledConnection, error)
	ReturnConnection(conn *network.PooledConnection)
	ClosePool() error
	GetPoolStats() network.PoolStats
	GetPoolMetrics() network.PoolMetrics
	IsPoolClosed() bool
	HealthCheck() error
}