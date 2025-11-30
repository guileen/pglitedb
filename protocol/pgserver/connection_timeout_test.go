package pgserver

import (
	"net"
	"testing"
	"time"

	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/sql"
)

// MockConnectionFactory implements ConnectionFactory for testing
type MockConnectionFactory struct {
	delay time.Duration
	fail  bool
}

func (mcf *MockConnectionFactory) CreateConnection(ctx interface{}) (net.Conn, error) {
	if mcf.fail {
		return nil, &net.OpError{Op: "connect", Err: &timeoutError{}}
	}
	time.Sleep(mcf.delay)
	return &mockConn{}, nil
}

// mockConn implements net.Conn for testing
type mockConn struct {
	closed bool
}

func (mc *mockConn) Read(b []byte) (n int, err error) {
	if mc.closed {
		return 0, &net.OpError{Op: "read", Err: &timeoutError{}}
	}
	// Simulate timeout after a short delay
	time.Sleep(100 * time.Millisecond)
	return 0, &net.OpError{Op: "read", Err: &timeoutError{}}
}

func (mc *mockConn) Write(b []byte) (n int, err error) {
	if mc.closed {
		return 0, &net.OpError{Op: "write", Err: &timeoutError{}}
	}
	return len(b), nil
}

func (mc *mockConn) Close() error {
	mc.closed = true
	return nil
}

func (mc *mockConn) LocalAddr() net.Addr  { return &net.TCPAddr{} }
func (mc *mockConn) RemoteAddr() net.Addr { return &net.TCPAddr{} }

func (mc *mockConn) SetDeadline(t time.Time) error      { return nil }
func (mc *mockConn) SetReadDeadline(t time.Time) error  { return nil }
func (mc *mockConn) SetWriteDeadline(t time.Time) error { return nil }

// timeoutError implements net.Error for testing
type timeoutError struct{}

func (te *timeoutError) Error() string   { return "timeout" }
func (te *timeoutError) Timeout() bool   { return true }
func (te *timeoutError) Temporary() bool { return false }

func TestConnectionHandlerTimeout(t *testing.T) {
	// Create a connection handler with short timeouts for testing
	parser := sql.NewPGParser()
	queryProcessor := components.NewQueryProcessor(nil, parser, nil)
	statementManager := components.NewPreparedStatementManager(parser)
	
	// Short timeouts for testing
	connectionHandler := components.NewConnectionHandlerWithTimeout(
		queryProcessor,
		statementManager,
		parser,
		100*time.Millisecond, // connection timeout
		50*time.Millisecond,  // idle timeout
		1*time.Second,        // max lifetime
	)
	
	// Create a mock connection that will timeout
	mockConn := &mockConn{}
	
	// This should timeout quickly due to the short idle timeout
	err := connectionHandler.HandleConnection(mockConn)
	
	// We expect an error due to timeout
	if err == nil {
		t.Error("Expected timeout error, got nil")
	}
	
	// Check that the connection was closed
	if !mockConn.closed {
		t.Error("Expected connection to be closed")
	}
}

func TestServerConfigTimeouts(t *testing.T) {
	// Test that server configuration properly sets timeout values
	cfg := config.DefaultServerConfig()
	
	// Verify default timeout values
	if cfg.ConnectionTimeout != 30*time.Second {
		t.Errorf("Expected ConnectionTimeout 30s, got %v", cfg.ConnectionTimeout)
	}
	
	if cfg.IdleTimeout != 5*time.Minute {
		t.Errorf("Expected IdleTimeout 5m, got %v", cfg.IdleTimeout)
	}
	
	if cfg.MaxLifetime != 1*time.Hour {
		t.Errorf("Expected MaxLifetime 1h, got %v", cfg.MaxLifetime)
	}
	
	// Test validation
	cfg.ConnectionTimeout = 0
	cfg.IdleTimeout = 0
	cfg.MaxLifetime = 0
	
	ValidateConfig(cfg)
	
	// Should be set to defaults
	if cfg.ConnectionTimeout != 30*time.Second {
		t.Errorf("Expected validated ConnectionTimeout 30s, got %v", cfg.ConnectionTimeout)
	}
	
	if cfg.IdleTimeout != 5*time.Minute {
		t.Errorf("Expected validated IdleTimeout 5m, got %v", cfg.IdleTimeout)
	}
	
	if cfg.MaxLifetime != 1*time.Hour {
		t.Errorf("Expected validated MaxLifetime 1h, got %v", cfg.MaxLifetime)
	}
}