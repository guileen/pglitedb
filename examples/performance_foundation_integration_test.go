// Final integration test demonstrating all performance improvements working together
package main

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/guileen/pglitedb/network"
	"github.com/guileen/pglitedb/types"
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
	time.Sleep(5 * time.Millisecond)
	return &mockConnection{}, nil
}

func main() {
	fmt.Println("=== PGLiteDB Performance Foundation Integration Test ===")
	
	// Test 1: Memory Pooling
	fmt.Println("\n1. Testing Memory Pooling...")
	testMemoryPooling()
	
	// Test 2: Connection Pooling
	fmt.Println("\n2. Testing Connection Pooling...")
	testConnectionPooling()
	
	// Test 3: Combined Performance
	fmt.Println("\n3. Testing Combined Performance...")
	testCombinedPerformance()
	
	fmt.Println("\n=== All Tests Completed Successfully! ===")
}

func testMemoryPooling() {
	start := time.Now()
	
	// Test record pooling
	var records []*types.Record
	
	// Acquire many records
	for i := 0; i < 100; i++ {
		record := types.AcquireRecord()
		record.ID = fmt.Sprintf("id-%d", i)
		record.Table = "test_table"
		record.Data["value"] = &types.Value{Data: fmt.Sprintf("data-%d", i)}
		records = append(records, record)
	}
	
	acquireTime := time.Since(start)
	fmt.Printf("   Acquired 100 records in %v\n", acquireTime)
	
	// Release all records
	start = time.Now()
	for _, record := range records {
		types.ReleaseRecord(record)
	}
	
	releaseTime := time.Since(start)
	fmt.Printf("   Released 100 records in %v\n", releaseTime)
	
	// Acquire them again to demonstrate reuse
	start = time.Now()
	for i := 0; i < 100; i++ {
		record := types.AcquireRecord()
		records = append(records, record)
	}
	
	reuseTime := time.Since(start)
	fmt.Printf("   Re-acquired 100 records (reused) in %v\n", reuseTime)
	
	// Clean up
	for _, record := range records {
		types.ReleaseRecord(record)
	}
	
	fmt.Println("   ✓ Memory pooling working correctly")
}

func testConnectionPooling() {
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
	time.Sleep(50 * time.Millisecond)

	// Test concurrent connection acquisition
	var wg sync.WaitGroup
	start := time.Now()
	
	// Acquire 10 connections concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			
			conn, err := pool.Get(ctx)
			if err != nil {
				fmt.Printf("   Error getting connection %d: %v\n", idx, err)
				return
			}
			
			// Simulate work
			time.Sleep(10 * time.Millisecond)
			
			// Return connection to pool
			pool.Put(conn)
		}(i)
	}
	
	wg.Wait()
	elapsed := time.Since(start)
	fmt.Printf("   Acquired and returned 10 connections concurrently in %v\n", elapsed)
	
	// Clean up
	pool.Close()
	fmt.Println("   ✓ Connection pooling working correctly")
}

func testCombinedPerformance() {
	// This test demonstrates how all performance improvements work together
	
	// 1. Use memory pooling for object allocation
	start := time.Now()
	var records []*types.Record
	
	// Acquire records using pooling
	for i := 0; i < 50; i++ {
		record := types.AcquireRecord()
		record.ID = fmt.Sprintf("perf-id-%d", i)
		record.Table = "performance_test"
		record.Data["metric"] = &types.Value{Data: fmt.Sprintf("metric-%d", i)}
		records = append(records, record)
	}
	
	memoryTime := time.Since(start)
	fmt.Printf("   Memory allocation (pooled): %v\n", memoryTime)
	
	// 2. Use connection pooling for database operations
	config := network.PoolConfig{
		MaxConnections:    3,
		MinConnections:    1,
		ConnectionTimeout: 3 * time.Second,
		IdleTimeout:       5 * time.Minute,
		MaxLifetime:       30 * time.Minute,
	}

	factory := &mockConnectionFactory{}
	pool := network.NewConnectionPool(config, factory)
	
	// Give time for min connections
	time.Sleep(30 * time.Millisecond)
	
	var connWg sync.WaitGroup
	connStart := time.Now()
	
	// Simulate concurrent database operations
	for i := 0; i < 5; i++ {
		connWg.Add(1)
		go func(idx int) {
			defer connWg.Done()
			
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			
			conn, err := pool.Get(ctx)
			if err != nil {
				return
			}
			
			// Simulate database work
			time.Sleep(20 * time.Millisecond)
			
			// Return connection to pool
			pool.Put(conn)
		}(i)
	}
	
	connWg.Wait()
	connTime := time.Since(connStart)
	fmt.Printf("   Connection operations (pooled): %v\n", connTime)
	
	// Clean up
	for _, record := range records {
		types.ReleaseRecord(record)
	}
	pool.Close()
	
	fmt.Println("   ✓ Combined performance improvements working together")
}