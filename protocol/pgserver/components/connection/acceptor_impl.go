package connection

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// isTemporaryError checks if an error is temporary and can be retried
func isTemporaryError(err error) bool {
	if netErr, ok := err.(net.Error); ok && netErr.Temporary() {
		return true
	}
	// Check for specific temporary error conditions
	errStr := err.Error()
	return strings.Contains(errStr, "too many open files") ||
		   strings.Contains(errStr, "temporary failure") ||
		   strings.Contains(errStr, "resource temporarily unavailable")
}

// ConnectionAcceptorImpl implements the ConnectionAcceptanceInterface
type ConnectionAcceptorImpl struct {
	connectionCount int64
	mu              sync.Mutex
	stopped         bool
	wg              sync.WaitGroup // WaitGroup to track active connections
}

// NewConnectionAcceptorImpl creates a new connection acceptor implementation
func NewConnectionAcceptorImpl() interfaces.ConnectionAcceptanceInterface {
	return &ConnectionAcceptorImpl{}
}

// AcceptConnections handles incoming connections from a listener
func (ca *ConnectionAcceptorImpl) AcceptConnections(listener net.Listener, connectionHandler interfaces.ConnectionHandlerInterface) error {
	logger.Info("Starting to accept connections", "address", listener.Addr().String())
	
	// Verify that we have a valid listener
	if listener == nil {
		logger.Error("Listener is nil in AcceptConnections")
		return fmt.Errorf("listener is nil")
	}
	
	addr := listener.Addr()
	if addr == nil {
		logger.Error("Listener address is nil in AcceptConnections")
		return fmt.Errorf("listener address is nil")
	}
	
	logger.Info("AcceptConnections: Listener details", "network", addr.Network(), "address", addr.String())

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
			
			// Check if this is a temporary error that we can retry
			if isTemporaryError(err) {
				logger.Warn("Temporary error accepting connection, retrying", "error", err)
				time.Sleep(10 * time.Millisecond)
				continue
			}

			logger.Error("Failed to accept connection", "error", err)
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		// Increment connection count
		atomic.AddInt64(&ca.connectionCount, 1)
		logger.Info("Accepted new connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String(), "connection_count", atomic.LoadInt64(&ca.connectionCount))

		// Handle connection in a goroutine
		ca.wg.Add(1)
		go func() {
			defer ca.wg.Done()
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

// StopAcceptingConnections stops accepting new connections and waits for active connections to finish
func (ca *ConnectionAcceptorImpl) StopAcceptingConnections() {
	ca.mu.Lock()
	defer ca.mu.Unlock()
	ca.stopped = true
	
	// Wait for all active connections to finish with a timeout
	done := make(chan struct{})
	go func() {
		ca.wg.Wait()
		close(done)
	}()
	
	// Wait for connections to finish or timeout
	select {
	case <-done:
		// All connections finished
	case <-time.After(5 * time.Second):
		// Timeout - log warning but continue shutdown
		logger.Warn("Timeout waiting for connections to finish, proceeding with shutdown")
	}
}

// GetConnectionCount returns the number of active connections
func (ca *ConnectionAcceptorImpl) GetConnectionCount() int {
	return int(atomic.LoadInt64(&ca.connectionCount))
}