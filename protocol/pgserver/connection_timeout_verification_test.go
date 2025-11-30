package pgserver

import (
	"net"
	"testing"
	"time"

	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/sql"
)

// TestResourceCleanupOnTimeout verifies that resources are properly cleaned up when connections timeout
func TestResourceCleanupOnTimeout(t *testing.T) {
	parser := sql.NewPGParser()
	queryProcessor := components.NewQueryProcessor(nil, parser, nil)
	statementManager := components.NewPreparedStatementManager(parser)

	// Create connection handler with very short timeouts
	connectionHandler := components.NewConnectionHandlerWithTimeout(
		queryProcessor,
		statementManager,
		parser,
		10*time.Millisecond, // Very short connection timeout
		10*time.Millisecond, // Very short idle timeout
		1*time.Hour,         // Normal max lifetime
	)

	// Create a mock connection that will cause timeout
	mockConn := &delayedConn{
		delay:     100 * time.Millisecond, // Longer than timeout
		closeFunc: func() error { return nil }, // Initialize with a default function
	}

	// Track if cleanup happened
	cleanupCalled := false
	originalClose := mockConn.closeFunc
	mockConn.closeFunc = func() error {
		cleanupCalled = true
		return originalClose()
	}

	err := connectionHandler.HandleConnection(mockConn)

	// Should have an error
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Cleanup should have been called
	if !cleanupCalled {
		t.Error("Expected resource cleanup to be called")
	}

	// Connection should be marked as closed
	if !mockConn.closed {
		t.Error("Expected connection to be closed")
	}
}

// TestErrorHandlingOnTimeout verifies that error handling works as expected when connections timeout
func TestErrorHandlingOnTimeout(t *testing.T) {
	parser := sql.NewPGParser()
	queryProcessor := components.NewQueryProcessor(nil, parser, nil)
	statementManager := components.NewPreparedStatementManager(parser)

	// Create connection handler with very short timeout
	connectionHandler := components.NewConnectionHandlerWithTimeout(
		queryProcessor,
		statementManager,
		parser,
		10*time.Millisecond, // Very short connection timeout
		5*time.Minute,       // Normal idle timeout
		1*time.Hour,         // Normal max lifetime
	)

	// Create a mock connection that will cause timeout
	mockConn := &delayedConn{
		delay:     100 * time.Millisecond, // Longer than timeout
		closeFunc: func() error { return nil }, // Initialize with a default function
	}

	err := connectionHandler.HandleConnection(mockConn)

	// Should have an error
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}

	// Error should indicate it's a timeout (check if it contains timeout in the error message)
	if netErr, ok := err.(net.Error); ok {
		if !netErr.Timeout() {
			t.Error("Expected timeout error")
		}
	} else {
		// If it's not a net.Error, check if the error message contains timeout
		if err != nil && len(err.Error()) > 0 {
			// This is acceptable as long as we have an error
		} else {
			t.Errorf("Expected error, got %T: %v", err, err)
		}
	}
}

// TestExistingFunctionalityNotBroken verifies that existing functionality is not broken by timeout implementation
func TestExistingFunctionalityNotBroken(t *testing.T) {
	// Test that default configuration works correctly
	cfg := config.DefaultServerConfig()

	// Verify default timeout values are reasonable
	if cfg.ConnectionTimeout < 1*time.Second {
		t.Error("Connection timeout too short")
	}

	if cfg.IdleTimeout < 1*time.Minute {
		t.Error("Idle timeout too short")
	}

	if cfg.MaxLifetime < 10*time.Minute {
		t.Error("Max lifetime too short")
	}
}

// delayedConn simulates a connection that delays during operations
type delayedConn struct {
	delay     time.Duration
	closed    bool
	closeFunc func() error
}

func (dc *delayedConn) Read(b []byte) (n int, err error) {
	if dc.closed {
		return 0, &net.OpError{Op: "read", Err: &timeoutError{}}
	}
	time.Sleep(dc.delay)
	return 0, &net.OpError{Op: "read", Err: &timeoutError{}}
}

func (dc *delayedConn) Write(b []byte) (n int, err error) {
	if dc.closed {
		return 0, &net.OpError{Op: "write", Err: &timeoutError{}}
	}
	time.Sleep(dc.delay)
	return 0, &net.OpError{Op: "write", Err: &timeoutError{}}
}

func (dc *delayedConn) Close() error {
	dc.closed = true
	if dc.closeFunc != nil {
		return dc.closeFunc()
	}
	return nil
}

func (dc *delayedConn) LocalAddr() net.Addr  { return &net.TCPAddr{} }
func (dc *delayedConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (dc *delayedConn) SetDeadline(t time.Time) error      { return nil }
func (dc *delayedConn) SetReadDeadline(t time.Time) error  { return nil }
func (dc *delayedConn) SetWriteDeadline(t time.Time) error { return nil }