package network

import (
	"context"
	"testing"
	"time"
)

func TestConnectionPoolWithMetrics(t *testing.T) {
	// Create a mock connection factory
	factory := NewMockConnectionFactory(false, 0)
	
	// Create pool config with metrics enabled
	config := PoolConfig{
		MaxConnections:    5,
		MinConnections:    2,
		ConnectionTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxLifetime:       1 * time.Hour,
		MaxIdleConns:      3,
		HealthCheckPeriod: 1 * time.Minute,
		MetricsEnabled:    true,
	}
	
	// Create connection pool
	pool := NewConnectionPool(config, factory)
	defer pool.Close()
	
	// Wait a bit for initial connections to be created
	time.Sleep(100 * time.Millisecond)
	
	// Test acquiring connections
	ctx := context.Background()
	conn1, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	
	conn2, err := pool.Get(ctx)
	if err != nil {
		t.Fatalf("Failed to get connection: %v", err)
	}
	
	// Test releasing connections
	conn1.Close()
	conn2.Close()
	
	// Get metrics
	metrics := pool.GetMetrics()
	stats := pool.Stats()
	
	// Verify basic metrics
	if metrics.CurrentConnections <= 0 {
		t.Errorf("Expected positive current connections, got %d", metrics.CurrentConnections)
	}
	
	if stats.Hits+stats.Misses <= 0 {
		t.Error("Expected some connection operations")
	}
	
	// Verify that we can get formatted metrics
	if metrics.ConnectionHits < 0 {
		t.Error("Connection hits should be non-negative")
	}
	
	if metrics.HitRate < 0 || metrics.HitRate > 100 {
		t.Errorf("Hit rate should be between 0 and 100, got %f", metrics.HitRate)
	}
}

func TestConnectionPoolWithErrorHandling(t *testing.T) {
	// Create a mock connection factory that fails initially
	factory := NewMockConnectionFactory(true, 2)
	
	config := PoolConfig{
		MaxConnections:    3,
		MinConnections:    1,
		ConnectionTimeout: 1 * time.Second,
		IdleTimeout:       30 * time.Second,
		MaxLifetime:       1 * time.Hour,
		MaxIdleConns:      2,
		HealthCheckPeriod: 1 * time.Minute,
	}
	
	pool := NewConnectionPool(config, factory)
	defer pool.Close()
	
	// Wait a bit for initial connection attempts
	time.Sleep(100 * time.Millisecond)
	
	// First few attempts should fail
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	
	_, err := pool.Get(ctx)
	if err == nil {
		t.Error("Expected error from failed connection")
	}
	
	// Verify it's a connection pool error
	if !IsConnectionPoolError(err) {
		t.Errorf("Expected ConnectionPoolError, got %T", err)
	}
	
	// Next attempt should succeed (factory stops failing after 2 attempts)
	time.Sleep(100 * time.Millisecond)
	
	// Create a fresh context for the second attempt
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	
	conn, err := pool.Get(ctx2)
	if err != nil {
		t.Fatalf("Expected successful connection, got %v", err)
	}
	
	conn.Close()
	
	// Check that we recorded errors
	stats := pool.Stats()
	if stats.ConnectionErrors == 0 {
		t.Error("Expected connection errors to be recorded")
	}
}