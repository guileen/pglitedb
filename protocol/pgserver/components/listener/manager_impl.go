package listener

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

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