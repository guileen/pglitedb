package interfaces

import (
	"net"
)

// ConnectionManagementInterface defines the interface for connection management operations
type ConnectionManagementInterface interface {
	GetListener() net.Listener
	SetListener(listener net.Listener)
	CloseListener() error
	GetConnectionPool() interface{}
	CloseConnectionPool() error
	IsClosed() bool
	HealthCheck() error
}