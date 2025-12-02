package listener

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/guileen/pglitedb/logger"
)

// ListenerManager handles listener management for different protocols
type ListenerManager struct {
	listener net.Listener
	mu       sync.Mutex
	closed   bool
}

// NewListenerManager creates a new listener manager
func NewListenerManager() *ListenerManager {
	return &ListenerManager{}
}

// StartTCP starts the listener on the specified TCP port
func (lm *ListenerManager) StartTCP(port string) error {
	logger.Info("Starting TCP listener", "port", port)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.closed {
		return fmt.Errorf("listener manager is closed")
	}

	// Check if listener is already started
	if lm.listener != nil {
		return fmt.Errorf("listener already started")
	}

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("Failed to start TCP listener", "error", err, "port", port)
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}

	lm.listener = listener
	logger.Info("TCP listener started successfully", "port", port)
	return nil
}

// StartUnix starts the listener on the specified Unix socket
func (lm *ListenerManager) StartUnix(socketPath string) error {
	logger.Info("Starting Unix socket listener", "socketPath", socketPath)

	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.closed {
		return fmt.Errorf("listener manager is closed")
	}

	// Check if listener is already started
	if lm.listener != nil {
		return fmt.Errorf("listener already started")
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
func (lm *ListenerManager) GetListener() net.Listener {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.listener
}

// SetListener sets the listener
func (lm *ListenerManager) SetListener(listener net.Listener) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.listener = listener
}

// CloseListener closes the listener
func (lm *ListenerManager) CloseListener() error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.listener != nil {
		err := lm.listener.Close()
		if err != nil {
			logger.Error("Error closing listener", "error", err)
			return err
		}
		logger.Info("Listener closed successfully")
	}
	return nil
}

// GetListenerAddress returns the address the listener is listening on
func (lm *ListenerManager) GetListenerAddress() net.Addr {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if lm.listener != nil {
		return lm.listener.Addr()
	}
	return nil
}

// IsClosed returns whether the listener manager is closed
func (lm *ListenerManager) IsClosed() bool {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	return lm.closed
}

// SetClosed sets the closed state
func (lm *ListenerManager) SetClosed(closed bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.closed = closed
}

// HealthCheck performs a health check on the listener manager
func (lm *ListenerManager) HealthCheck() error {
	// Simple health check implementation
	return nil
}