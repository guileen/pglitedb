package config

import (
	"time"
)

// ServerConfig holds configuration options for the PostgreSQL server
type ServerConfig struct {
	// Network configuration
	TCPHost string
	TCPPort string
	UnixSocketPath string
	
	// Connection pool configuration
	MaxConnections int
	MinConnections int
	ConnectionTimeout time.Duration
	IdleTimeout time.Duration
	MaxLifetime time.Duration
	MaxIdleConns int
	HealthCheckPeriod time.Duration
	
	// Adaptive pooling configuration
	AdaptivePoolingEnabled bool
	TargetHitRate float64
	MinHitRateThreshold float64
	MaxHitRateThreshold float64
	AdaptationInterval time.Duration
	ExpansionFactor float64
	ContractionFactor float64
	MaxAdaptiveConnections int
	MinAdaptiveConnections int
	
	// Buffer pool configuration
	BufferSizes []int
	
	// Profiling configuration
	ProfilingPort string
	
	// Logging configuration
	LogLevel string
}

// DefaultServerConfig returns a ServerConfig with sensible defaults
func DefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		TCPHost: "localhost",
		TCPPort: "5432",
		UnixSocketPath: "",
		
		// Connection pool defaults
		MaxConnections: 100,
		MinConnections: 20,
		ConnectionTimeout: 30 * time.Second,
		IdleTimeout: 5 * time.Minute,
		MaxLifetime: 1 * time.Hour,
		MaxIdleConns: 50,
		HealthCheckPeriod: 1 * time.Minute,
		
		// Adaptive pooling defaults
		AdaptivePoolingEnabled: true,
		TargetHitRate: 95.0,
		MinHitRateThreshold: 80.0,
		MaxHitRateThreshold: 99.0,
		AdaptationInterval: 30 * time.Second,
		ExpansionFactor: 1.5,
		ContractionFactor: 0.8,
		MaxAdaptiveConnections: 200,
		MinAdaptiveConnections: 10,
		
		// Buffer pool defaults
		BufferSizes: []int{512, 1024, 2048, 4096, 8192, 16384},
		
		// Profiling defaults
		ProfilingPort: "",
		
		// Logging defaults
		LogLevel: "INFO",
	}
}