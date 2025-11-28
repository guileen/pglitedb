package network

import (
	"context"
	"fmt"
	"net"
	"time"
)

// TCPConnectionFactory implements ConnectionFactory for TCP connections
type TCPConnectionFactory struct {
	address string
	timeout time.Duration
}

// NewTCPConnectionFactory creates a new TCP connection factory
func NewTCPConnectionFactory(address string, timeout time.Duration) *TCPConnectionFactory {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &TCPConnectionFactory{
		address: address,
		timeout: timeout,
	}
}

// CreateConnection creates a new TCP connection
func (cf *TCPConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	// Use the context timeout if available, otherwise use factory timeout
	timeout := cf.timeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, &ConnectionPoolError{
				Op:  "dial",
				Err: context.DeadlineExceeded,
			}
		}
	}

	// Create connection with timeout
	conn, err := net.DialTimeout("tcp", cf.address, timeout)
	if err != nil {
		return nil, &ConnectionPoolError{
			Op:  "dial",
			Err: fmt.Errorf("failed to dial %s: %w", cf.address, err),
		}
	}

	return conn, nil
}

// UnixConnectionFactory implements ConnectionFactory for Unix socket connections
type UnixConnectionFactory struct {
	socketPath string
	timeout    time.Duration
}

// NewUnixConnectionFactory creates a new Unix socket connection factory
func NewUnixConnectionFactory(socketPath string, timeout time.Duration) *UnixConnectionFactory {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &UnixConnectionFactory{
		socketPath: socketPath,
		timeout:    timeout,
	}
}

// CreateConnection creates a new Unix socket connection
func (cf *UnixConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	// Use the context timeout if available, otherwise use factory timeout
	timeout := cf.timeout
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return nil, &ConnectionPoolError{
				Op:  "dial",
				Err: context.DeadlineExceeded,
			}
		}
	}

	// Create connection with timeout
	conn, err := net.DialTimeout("unix", cf.socketPath, timeout)
	if err != nil {
		return nil, &ConnectionPoolError{
			Op:  "dial",
			Err: fmt.Errorf("failed to dial unix socket %s: %w", cf.socketPath, err),
		}
	}

	return conn, nil
}

// MockConnectionFactory implements ConnectionFactory for testing
type MockConnectionFactory struct {
	failCount    int
	currentCount int
	shouldFail   bool
}

// NewMockConnectionFactory creates a new mock connection factory for testing
func NewMockConnectionFactory(shouldFail bool, failCount int) *MockConnectionFactory {
	return &MockConnectionFactory{
		shouldFail: shouldFail,
		failCount:  failCount,
	}
}

// CreateConnection creates a mock connection for testing
func (cf *MockConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	cf.currentCount++
	
	if cf.shouldFail && cf.currentCount <= cf.failCount {
		return nil, &ConnectionPoolError{
			Op:  "mock_dial",
			Err: fmt.Errorf("mock connection failure %d", cf.currentCount),
		}
	}

	// Return a mock connection (in real implementation, this would be a real connection)
	return &mockConn{}, nil
}

// mockConn is a mock connection for testing
type mockConn struct {
	closed bool
}

func (mc *mockConn) Read(b []byte) (n int, err error) {
	if mc.closed {
		return 0, &net.OpError{Op: "read", Err: &net.AddrError{Err: "connection closed"}}
	}
	return 0, nil
}

func (mc *mockConn) Write(b []byte) (n int, err error) {
	if mc.closed {
		return 0, &net.OpError{Op: "write", Err: &net.AddrError{Err: "connection closed"}}
	}
	return len(b), nil
}

func (mc *mockConn) Close() error {
	mc.closed = true
	return nil
}

func (mc *mockConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 12345}
}

func (mc *mockConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 5432}
}

func (mc *mockConn) SetDeadline(t time.Time) error {
	return nil
}

func (mc *mockConn) SetReadDeadline(t time.Time) error {
	return nil
}

func (mc *mockConn) SetWriteDeadline(t time.Time) error {
	return nil
}