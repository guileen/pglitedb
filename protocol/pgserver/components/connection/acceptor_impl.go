package connection

import (
	"net"
	"sync"
	"sync/atomic"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// ConnectionAcceptorImpl implements the ConnectionAcceptanceInterface
type ConnectionAcceptorImpl struct {
	connectionCount int64
	mu              sync.Mutex
	stopped         bool
}

// NewConnectionAcceptorImpl creates a new connection acceptor implementation
func NewConnectionAcceptorImpl() interfaces.ConnectionAcceptanceInterface {
	return &ConnectionAcceptorImpl{}
}

// AcceptConnections handles incoming connections from a listener
func (ca *ConnectionAcceptorImpl) AcceptConnections(listener net.Listener, connectionHandler interfaces.ConnectionHandlerInterface) error {
	logger.Info("Starting to accept connections", "address", listener.Addr().String())

	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if acceptor is stopped
			ca.mu.Lock()
			stopped := ca.stopped
			ca.mu.Unlock()

			if stopped {
				logger.Info("Connection acceptor is stopping, stopping connection acceptance")
				return nil
			}

			logger.Error("Failed to accept connection", "error", err)
			continue
		}

		// Increment connection count
		atomic.AddInt64(&ca.connectionCount, 1)
		logger.Info("Accepted new connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String(), "connection_count", atomic.LoadInt64(&ca.connectionCount))

		// Handle connection in a goroutine
		go func() {
			defer func() {
				atomic.AddInt64(&ca.connectionCount, -1)
				logger.Info("Connection closed", "remote_addr", conn.RemoteAddr().String(), "connection_count", atomic.LoadInt64(&ca.connectionCount))
			}()

			if err := connectionHandler.HandleConnection(conn); err != nil {
				logger.Error("Error handling connection", "error", err, "remote_addr", conn.RemoteAddr().String())
			}
		}()
	}
}

// StartAcceptingConnections starts accepting connections in a goroutine
func (ca *ConnectionAcceptorImpl) StartAcceptingConnections(listener net.Listener, connectionHandler interfaces.ConnectionHandlerInterface) {
	go func() {
		if err := ca.AcceptConnections(listener, connectionHandler); err != nil {
			logger.Error("Error accepting connections", "error", err)
		}
	}()
}

// StopAcceptingConnections stops accepting new connections
func (ca *ConnectionAcceptorImpl) StopAcceptingConnections() {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.stopped = true
}

// GetConnectionCount returns the number of active connections
func (ca *ConnectionAcceptorImpl) GetConnectionCount() int {
	return int(atomic.LoadInt64(&ca.connectionCount))
}