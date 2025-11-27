package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/guileen/pglitedb/network"
)

// Mock connection for testing
type mockConnection struct{}

func (c *mockConnection) Read(b []byte) (n int, err error) { return 0, nil }
func (c *mockConnection) Write(b []byte) (n int, err error) { return len(b), nil }
func (c *mockConnection) Close() error { return nil }
func (c *mockConnection) LocalAddr() net.Addr { return nil }
func (c *mockConnection) RemoteAddr() net.Addr { return nil }
func (c *mockConnection) SetDeadline(t time.Time) error { return nil }
func (c *mockConnection) SetReadDeadline(t time.Time) error { return nil }
func (c *mockConnection) SetWriteDeadline(t time.Time) error { return nil }

// Mock connection factory for testing
type mockConnectionFactory struct{}

func (f *mockConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	// Simulate connection creation delay
	time.Sleep(10 * time.Millisecond)
	return &mockConnection{}, nil
}

func main() {
	fmt.Println("Testing connection pooling...")
	
	// Create a connection pool
	config := network.PoolConfig{
		MaxConnections:    5,
		MinConnections:    2,
		ConnectionTimeout: 5 * time.Second,
		IdleTimeout:       10 * time.Minute,
		MaxLifetime:       1 * time.Hour,
	}

	// Create a mock factory
	factory := &mockConnectionFactory{}

	// Create the pool
	pool := network.NewConnectionPool(config, factory)
	
	// Give some time for min connections to be created
	time.Sleep(100 * time.Millisecond)

	// Test getting connections
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	conn1, err := pool.Get(ctx)
	if err != nil {
		fmt.Printf("Error getting connection 1: %v\n", err)
	} else {
		fmt.Println("Got connection 1")
		// Return connection to pool
		pool.Put(conn1)
		fmt.Println("Returned connection 1")
	}

	conn2, err := pool.Get(ctx)
	if err != nil {
		fmt.Printf("Error getting connection 2: %v\n", err)
	} else {
		fmt.Println("Got connection 2")
		// Return connection to pool
		pool.Put(conn2)
		fmt.Println("Returned connection 2")
	}

	// Clean up
	pool.Close()
	fmt.Println("Connection pool test completed")
}