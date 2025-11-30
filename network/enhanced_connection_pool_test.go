package network

import (
	"context"
	"net"
	"testing"
	"time"
)

// mockConnectionFactory is a mock connection factory for testing
type mockConnectionFactory struct {
	createDelay time.Duration
	shouldFail  bool
}

func (mcf *mockConnectionFactory) CreateConnection(ctx context.Context) (net.Conn, error) {
	if mcf.createDelay > 0 {
		time.Sleep(mcf.createDelay)
	}
	
	if mcf.shouldFail {
		return nil, &ConnectionPoolError{Op: "create", Err: ErrTimeout}
	}
	
	// Use existing mock connection
	return &mockConn{}, nil
}

func TestEnhancedConnectionPool_Create(t *testing.T) {
	config := PoolConfig{
		MaxConnections: 10,
		MinConnections: 2,
		ConnectionTimeout: 1 * time.Second,
	}
	
	factory := &mockConnectionFactory{}
	pool := NewEnhancedConnectionPool(config, factory)
	
	if pool == nil {
		t.Fatal("Failed to create enhanced connection pool")
	}
	
	// Check that adaptive defaults were set
	if pool.config.TargetHitRate != 95.0 {
		t.Errorf("Expected TargetHitRate 95.0, got %f", pool.config.TargetHitRate)
	}
	
	if pool.config.MinHitRateThreshold != 85.0 {
		t.Errorf("Expected MinHitRateThreshold 85.0, got %f", pool.config.MinHitRateThreshold)
	}
}

func TestEnhancedConnectionPool_GetConnection(t *testing.T) {
	config := PoolConfig{
		MaxConnections: 5,
		MinConnections: 1,
		ConnectionTimeout: 100 * time.Millisecond,
	}
	
	factory := &mockConnectionFactory{}
	pool := NewEnhancedConnectionPool(config, factory)
	
	// Get a connection
	ctx := context.Background()
	conn, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	
	if conn == nil {
		t.Fatal("Got nil connection")
	}
	
	// Return the connection
	pool.Put(conn)
	
	// Get another connection (should be from pool)
	conn2, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get second connection: %v", err)
	}
	
	if conn2 == nil {
		t.Fatal("Got nil connection on second get")
	}
	
	pool.Put(conn2)
}