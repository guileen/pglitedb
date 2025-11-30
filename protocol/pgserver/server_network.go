package pgserver

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/guileen/pglitedb/logger"
)

// StartTCP starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	logger.Info("Starting PostgreSQL server TCP listener", "port", port)
	
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("Failed to start TCP listener", "error", err, "port", port)
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on TCP port", "port", port)
	log.Printf("PostgreSQL server listening on TCP port %s", port)
	
	return s.acceptConnections()
}

// StartUnix starts the PostgreSQL server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socketPath string) error {
	logger.Info("Starting PostgreSQL server Unix socket listener", "socketPath", socketPath)
	
	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove existing socket file", "error", err, "socketPath", socketPath)
		log.Printf("Warning: failed to remove existing socket file: %v", err)
	}
	
	var err error
	s.listener, err = net.Listen("unix", socketPath)
	if err != nil {
		logger.Error("Failed to start Unix socket listener", "error", err, "socketPath", socketPath)
		return fmt.Errorf("failed to start Unix socket listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on Unix socket", "socketPath", socketPath)
	log.Printf("PostgreSQL server listening on Unix socket %s", socketPath)
	
	return s.acceptConnections()
}

// acceptConnections handles incoming connections
func (s *PostgreSQLServer) acceptConnections() error {
	connectionCount := 0
	for {
		conn, err := s.listener.Accept()
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
		
		go s.connectionHandler.HandleConnection(conn)
	}
}

// GetListenerAddress returns the address the server is listening on
func (s *PostgreSQLServer) GetListenerAddress() net.Addr {
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// IsClosed returns whether the server is closed
func (s *PostgreSQLServer) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
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