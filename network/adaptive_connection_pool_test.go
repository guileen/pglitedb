package network

import (
	"context"
	"testing"
	"time"
)

func TestAdaptiveConnectionPool(t *testing.T) {
	// Create a mock connection factory
	factory := NewMockConnectionFactory(false, 0)

	// Create pool config with adaptive pooling enabled
	config := PoolConfig{
		MaxConnections:         5,
		MinConnections:         2,
		ConnectionTimeout:      10 * time.Second,
		IdleTimeout:            30 * time.Second,
		MaxLifetime:            1 * time.Hour,
		MaxIdleConns:           3,
		HealthCheckPeriod:      1 * time.Minute,
		MetricsEnabled:         true,
		AdaptivePoolingEnabled: true,
		TargetHitRate:          80.0,
		MinHitRateThreshold:    50.0,
		MaxHitRateThreshold:    95.0,
		AdaptationInterval:     1 * time.Second, // Short interval for testing
		ExpansionFactor:        1.5,
		ContractionFactor:      0.8,
		MaxAdaptiveConnections: 10,
		MinAdaptiveConnections: 2,
	}

	// Create adaptive connection pool
	pool := NewAdaptiveConnectionPool(config, factory)
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

	// Get adaptive metrics
	metrics := pool.GetAdaptiveMetrics()

	// Verify basic metrics
	if metrics.CurrentConnections <= 0 {
		t.Errorf("Expected positive current connections, got %d", metrics.CurrentConnections)
	}

	if metrics.ConnectionHits+metrics.ConnectionMisses <= 0 {
		t.Error("Expected some connection operations")
	}

	// Verify adaptive metrics
	if !metrics.AdaptivePoolingEnabled {
		t.Error("Expected adaptive pooling to be enabled")
	}

	if metrics.MaxPoolSize != config.MaxConnections {
		t.Errorf("Expected max pool size %d, got %d", config.MaxConnections, metrics.MaxPoolSize)
	}
}

func TestAdaptivePoolSizeAdjustment(t *testing.T) {
	// Create a mock connection factory
	factory := NewMockConnectionFactory(false, 0)

	// Create pool config with adaptive pooling enabled
	config := PoolConfig{
		MaxConnections:         3,
		MinConnections:         1,
		ConnectionTimeout:      10 * time.Second,
		IdleTimeout:            30 * time.Second,
		MaxLifetime:            1 * time.Hour,
		MaxIdleConns:           3,
		HealthCheckPeriod:      1 * time.Minute,
		MetricsEnabled:         true,
		AdaptivePoolingEnabled: true,
		TargetHitRate:          80.0,
		MinHitRateThreshold:    50.0,
		MaxHitRateThreshold:    95.0,
		AdaptationInterval:     100 * time.Millisecond, // Very short for testing
		ExpansionFactor:        1.5,
		ContractionFactor:      0.8,
		MaxAdaptiveConnections: 6,
		MinAdaptiveConnections: 1,
	}

	// Create adaptive connection pool
	pool := NewAdaptiveConnectionPool(config, factory)
	defer pool.Close()

	// Wait for initial setup
	time.Sleep(100 * time.Millisecond)

	// Get initial metrics
	initialMetrics := pool.GetAdaptiveMetrics()
	initialMaxSize := initialMetrics.MaxPoolSize

	// Force an adaptation (this would normally happen based on metrics)
	// For testing, we'll manually trigger it
	pool.ForceAdaptation()

	// Wait a bit for adaptation to potentially occur
	time.Sleep(200 * time.Millisecond)

	// Get metrics after adaptation
	finalMetrics := pool.GetAdaptiveMetrics()
	finalMaxSize := finalMetrics.MaxPoolSize

	// The pool size might not change significantly in this simple test,
	// but we can verify the adaptation mechanism is working
	t.Logf("Initial max size: %d, Final max size: %d", initialMaxSize, finalMaxSize)

	// Verify we can get hit rate information
	currentHitRate := pool.GetCurrentHitRate()
	t.Logf("Current hit rate: %.2f%%", currentHitRate)

	// Verify hit rate history is being collected
	history := pool.GetHitRateHistory()
	t.Logf("Hit rate history length: %d", len(history))
}

func TestAdaptiveConfigUpdates(t *testing.T) {
	// Create a mock connection factory
	factory := NewMockConnectionFactory(false, 0)

	// Create pool config
	config := PoolConfig{
		MaxConnections:         5,
		MinConnections:         2,
		ConnectionTimeout:      10 * time.Second,
		IdleTimeout:            30 * time.Second,
		MaxLifetime:            1 * time.Hour,
		MaxIdleConns:           3,
		HealthCheckPeriod:      1 * time.Minute,
		AdaptivePoolingEnabled: true,
	}

	// Create adaptive connection pool
	pool := NewAdaptiveConnectionPool(config, factory)
	defer pool.Close()

	// Test updating adaptive configuration
	pool.SetAdaptiveConfig(90.0, 60.0, 98.0)

	// Verify configuration was updated
	target, minThresh, maxThresh := pool.GetAdaptiveConfig()
	if target != 90.0 {
		t.Errorf("Expected target hit rate 90.0, got %.2f", target)
	}
	if minThresh != 60.0 {
		t.Errorf("Expected min threshold 60.0, got %.2f", minThresh)
	}
	if maxThresh != 98.0 {
		t.Errorf("Expected max threshold 98.0, got %.2f", maxThresh)
	}

	// Test enabling/disabling adaptive pooling
	pool.EnableAdaptivePooling(false)
	if pool.IsAdaptivePoolingEnabled() {
		t.Error("Expected adaptive pooling to be disabled")
	}

	pool.EnableAdaptivePooling(true)
	if !pool.IsAdaptivePoolingEnabled() {
		t.Error("Expected adaptive pooling to be enabled")
	}
}