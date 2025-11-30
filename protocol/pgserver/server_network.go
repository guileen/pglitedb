package pgserver

import (
	"fmt"
	"log"
	"net"
	"time"

	"github.com/guileen/pglitedb/logger"
)

// acceptConnections handles incoming connections
func (s *PostgreSQLServer) acceptConnections() error {
	connectionCount := 0
	for {
		// Get the listener from the listener manager
		listener := s.listenerManager.GetListener()
		if listener == nil {
			logger.Error("Listener is nil, cannot accept connections")
			return fmt.Errorf("listener is nil")
		}
		
		conn, err := listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("Listener closed, exiting accept loop")
				return nil
			}
			
			logger.Error("Failed to accept connection", "error", err)
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		
		connectionCount++
		logger.Debug("Accepted new connection", "connection_count", connectionCount, "remote_addr", conn.RemoteAddr().String())
		
		// Add nil check to prevent panic
		if s.connectionHandler == nil {
			logger.Error("Connection handler is nil, closing connection")
			conn.Close()
			continue
		}
		
		// Type assert to the expected interface and call HandleConnection
		if handler, ok := s.connectionHandler.(interface {
			HandleConnection(conn net.Conn)
		}); ok {
			go handler.HandleConnection(conn)
		} else {
			logger.Error("Connection handler does not implement HandleConnection method, closing connection")
			conn.Close()
		}
	}
}

// GetListenerAddress returns the address the server is listening on
func (s *PostgreSQLServer) GetListenerAddress() net.Addr {
	return s.listenerManager.GetListenerAddress()
}

// IsClosed returns whether the server is closed
func (s *PostgreSQLServer) IsClosed() bool {
	return s.serverManager.IsClosed()
}

// GetConnectionCount returns the number of active connections
func (s *PostgreSQLServer) GetConnectionCount() int {
	// This would need to be implemented with proper connection tracking
	// For now, we'll return 0 as a placeholder
	return 0
}

// SetConnectionTimeout sets the connection timeout
func (s *PostgreSQLServer) SetConnectionTimeout(timeout time.Duration) {
	// Implementation would depend on how connection timeouts are managed
	// This is a placeholder for future implementation
}