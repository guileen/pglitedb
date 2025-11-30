package interfaces

import (
	"net"
)

// ListenerManagementInterface defines the interface for listener management operations
type ListenerManagementInterface interface {
	StartTCP(port string) error
	StartUnix(socketPath string) error
	GetListener() net.Listener
	SetListener(listener net.Listener)
	CloseListener() error
	GetListenerAddress() net.Addr
	IsClosed() bool
	HealthCheck() error
}