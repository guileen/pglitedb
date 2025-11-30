package interfaces

import (
	"net"
)

// ConnectionAcceptanceInterface defines the interface for connection acceptance operations
type ConnectionAcceptanceInterface interface {
	AcceptConnections(listener net.Listener, connectionHandler ConnectionHandlerInterface) error
	StartAcceptingConnections(listener net.Listener, connectionHandler ConnectionHandlerInterface)
	StopAcceptingConnections()
}