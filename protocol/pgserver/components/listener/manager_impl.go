package listener

import (
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// isPortAvailable checks if a port is available for binding
func isPortAvailable(port string) bool {
	// First check if we can connect to the port (it's in use)
	conn, err := net.DialTimeout("tcp", net.JoinHostPort("", port), time.Second)
	if err != nil {
		// If we can't connect, the port might be available
		// But we need to check if it's a "connection refused" error
		if strings.Contains(err.Error(), "connection refused") {
			return true
		}
		// For other errors, we assume the port is not available
		return false
	}
	// If we can connect, the port is definitely in use
	conn.Close()
	return false
}

// ListenerManagerImpl implements the ListenerManagerInterface
type ListenerManagerImpl struct {
	listener net.Listener
	mu       sync.Mutex
	closed   bool
}

// NewListenerManagerImpl creates a new listener manager implementation
func NewListenerManagerImpl() interfaces.ListenerManagementInterface {
	return &ListenerManagerImpl{}
}

// StartTCP starts the listener on the specified TCP port
func (lm *ListenerManagerImpl) StartTCP(port string) error {
	logger.Info("Starting TCP listener", "port", port)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.closed {
		return fmt.Errorf("listener manager is closed")
	}

	// Check if port is available before attempting to bind
	if !isPortAvailable(port) {
		logger.Warn("Port may be in use, will attempt to bind anyway", "port", port)
	}

	// Try to bind to IPv4 first, then fall back to default
	logger.Info("Attempting to bind to IPv4", "address", "0.0.0.0:"+port)
	listener, err := net.Listen("tcp4", "0.0.0.0:"+port)
	if err != nil {
		logger.Warn("Failed to start IPv4 TCP listener, trying default", "error", err, "port", port)
		// Fall back to default behavior
		logger.Info("Attempting to bind to default", "address", ":"+port)
		listener, err = net.Listen("tcp", ":"+port)
		if err != nil {
			logger.Error("Failed to start TCP listener", "error", err, "port", port)
			return fmt.Errorf("failed to start TCP listener: %w", err)
		}
	}
	
	// Verify that we actually got a listener
	if listener == nil {
		logger.Error("Listener is nil after successful net.Listen call", "port", port)
		return fmt.Errorf("listener is nil after successful net.Listen call")
	}
	
	// Log the actual address we're bound to
	actualAddr := listener.Addr()
	if actualAddr == nil {
		logger.Error("Listener address is nil", "port", port)
		return fmt.Errorf("listener address is nil")
	}
	
	logger.Info("Successfully created listener", "port", port, "address", actualAddr.String(), "network", actualAddr.Network())
	
	// Store the listener
	lm.listener = listener
	logger.Info("TCP listener stored in manager", "port", port, "address", listener.Addr().String())
	
	// Verify that the listener is stored
	if lm.listener == nil {
		logger.Error("Listener became nil after storing", "port", port)
		return fmt.Errorf("listener became nil after storing")
	}
	
	// Verify the stored listener's address
	storedAddr := lm.listener.Addr()
	if storedAddr == nil {
		logger.Error("Stored listener address is nil", "port", port)
		return fmt.Errorf("stored listener address is nil")
	}
	
	logger.Info("TCP listener verification successful", "port", port, "address", storedAddr.String())
	return nil
}

// StartUnix starts the listener on the specified Unix socket
func (lm *ListenerManagerImpl) StartUnix(socketPath string) error {
	logger.Info("Starting Unix socket listener", "socketPath", socketPath)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.closed {
		return fmt.Errorf("listener manager is closed")
	}

	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove existing socket file", "error", err, "socketPath", socketPath)
	}

	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		logger.Error("Failed to start Unix socket listener", "error", err, "socketPath", socketPath)
		return fmt.Errorf("failed to start Unix socket listener: %w", err)
	}

	lm.listener = listener
	logger.Info("Unix socket listener started successfully", "socketPath", socketPath)
	return nil
}

// GetListener returns the current listener
func (lm *ListenerManagerImpl) GetListener() net.Listener {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.listener
}

// SetListener sets the listener
func (lm *ListenerManagerImpl) SetListener(listener net.Listener) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.listener = listener
}

// CloseListener closes the listener
func (lm *ListenerManagerImpl) CloseListener() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.listener != nil {
		addr := lm.listener.Addr()
		logger.Info("Closing listener", "address", addr.String())
		err := lm.listener.Close()
		if err != nil {
			logger.Error("Error closing listener", "error", err, "address", addr.String())
			return err
		}
		logger.Info("Listener closed successfully", "address", addr.String())
		lm.listener = nil
	} else {
		logger.Info("No listener to close")
	}
	return nil
}

// GetListenerAddress returns the address the listener is listening on
func (lm *ListenerManagerImpl) GetListenerAddress() net.Addr {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.listener != nil {
		return lm.listener.Addr()
	}
	return nil
}

// IsClosed returns whether the listener manager is closed
func (lm *ListenerManagerImpl) IsClosed() bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.closed
}

// SetClosed sets the closed state
func (lm *ListenerManagerImpl) SetClosed(closed bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.closed = closed
}

// HealthCheck performs a health check on the listener manager
func (lm *ListenerManagerImpl) HealthCheck() error {
	// Simple health check implementation
	return nil
}