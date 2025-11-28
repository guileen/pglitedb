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

func (c *mockConnection) Read(b []byte) (n int, err error)  { return 0, nil }
func (c *mockConnection) Write(b []byte) (n int, err error) { return len(b), nil }
func (c *mockConnection) Close() error                      { return nil }
func (c *mockConnection) LocalAddr() net.Addr               { return nil }
func (c *mockConnection) RemoteAddr() net.Addr              { return nil }
func (c *mockConnection) SetDeadline(t time.Time) error     { return nil }
func (c *mockConnection) SetReadDeadline(t time.Time) error { return nil }
func (c *mockConnection) SetWriteDeadline(t time.Time) error {
	return nil
}

// Mock connection factory for testing
type mockConnectionFactory struct{}

func (f *mockConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	// Simulate connection creation delay
	time.Sleep(10 * time.Millisecond)
	return &mockConnection{}, nil
}

func main() {
	fmt.Println("Testing adaptive connection pooling...")

	// Create a connection pool with adaptive pooling enabled
	config := network.PoolConfig{
		MaxConnections:         5,
		MinConnections:         2,
		ConnectionTimeout:      5 * time.Second,
		IdleTimeout:            10 * time.Minute,
		MaxLifetime:            1 * time.Hour,
		AdaptivePoolingEnabled: true, // Enable adaptive pooling
		TargetHitRate:          80.0, // Target 80% hit rate
		MinHitRateThreshold:    50.0, // Expand if hit rate drops below 50%
		MaxHitRateThreshold:    95.0, // Contract if hit rate exceeds 95%
		AdaptationInterval:     30 * time.Second, // Adapt every 30 seconds
		ExpansionFactor:        1.5,  // Expand by 50%
		ContractionFactor:      0.8,  // Contract by 20%
		MaxAdaptiveConnections: 15,   // Maximum pool size
		MinAdaptiveConnections: 2,    // Minimum pool size
	}

	// Create a mock factory
	factory := &mockConnectionFactory{}

	// Create the adaptive connection pool
	pool := network.NewAdaptiveConnectionPool(config, factory)

	// Give some time for min connections to be created
	time.Sleep(100 * time.Millisecond)

	// Test getting connections
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Simulate some connection usage
	for i := 0; i < 10; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			fmt.Printf("Error getting connection %d: %v\n", i+1, err)
		} else {
			fmt.Printf("Got connection %d\n", i+1)
			// Return connection to pool after some simulated work
			time.Sleep(5 * time.Millisecond)
			pool.Put(conn)
			fmt.Printf("Returned connection %d\n", i+1)
		}
	}

	// Get adaptive metrics
	metrics := pool.GetAdaptiveMetrics()
	fmt.Printf("\nAdaptive Pool Metrics:\n")
	fmt.Printf("  Current Connections: %d\n", metrics.CurrentConnections)
	fmt.Printf("  Active Connections: %d\n", metrics.ActiveConnections)
	fmt.Printf("  Idle Connections: %d\n", metrics.IdleConnections)
	fmt.Printf("  Hit Rate: %.2f%%\n", metrics.HitRate)
	fmt.Printf("  Current Pool Size: %d\n", metrics.CurrentPoolSize)
	fmt.Printf("  Max Pool Size: %d\n", metrics.MaxPoolSize)
	fmt.Printf("  Average Hit Rate: %.2f%%\n", metrics.AverageHitRate)
	fmt.Printf("  Hit Rate History Length: %d\n", metrics.HitRateHistoryLength)
	fmt.Printf("  Time Since Last Adaptation: %v\n", metrics.TimeSinceLastAdapt)
	fmt.Printf("  Adaptive Pooling Enabled: %v\n", metrics.AdaptivePoolingEnabled)

	// Clean up
	pool.Close()
	fmt.Println("Adaptive connection pool test completed")
}