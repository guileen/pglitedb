package network

import (
	"context"
	"testing"
	"time"
)

func BenchmarkEnhancedConnectionPool_GetConnection(b *testing.B) {
	config := PoolConfig{
		MaxConnections: 100,
		MinConnections: 10,
		ConnectionTimeout: 100 * time.Millisecond,
	}
	
	factory := &mockConnectionFactory{}
	pool := NewEnhancedConnectionPool(config, factory)
	
	b.ResetTimer()
	
	ctx := context.Background()
	for i := 0; i < b.N; i++ {
		conn, err := pool.Get(ctx)
		if err != nil {
			b.Fatalf("Failed to get connection: %v", err)
		}
		pool.Put(conn)
	}
}

func BenchmarkEnhancedConnectionPool_PredictOptimalSize(b *testing.B) {
	config := PoolConfig{
		MaxConnections: 10,
		MinConnections: 2,
		ConnectionTimeout: 100 * time.Millisecond,
		MaxAdaptiveConnections: 20,
		MinAdaptiveConnections: 5,
	}
	
	factory := &mockConnectionFactory{}
	pool := NewEnhancedConnectionPool(config, factory)
	
	// Set up some hit rate history
	pool.hitRateHistory = []float64{90.0, 88.0, 85.0, 82.0, 80.0}
	
	b.ResetTimer()
	
	for i := 0; i < b.N; i++ {
		_ = pool.predictOptimalPoolSize()
	}
}