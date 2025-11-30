package pgserver

import (
	"fmt"
	"log/slog"
	"time"
	
	"github.com/guileen/pglitedb/pool"
	"github.com/guileen/pglitedb/logger"
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

// ApplyConfig applies the configuration to the server
func (s *PostgreSQLServer) ApplyConfig(config *ServerConfig) error {
	// Apply logging configuration
	if config.LogLevel != "" {
		// Convert string log level to slog.Level
		var level slog.Level
		switch config.LogLevel {
		case "DEBUG":
			level = slog.LevelDebug
		case "INFO":
			level = slog.LevelInfo
		case "WARN":
			level = slog.LevelWarn
		case "ERROR":
			level = slog.LevelError
		default:
			level = slog.LevelInfo
		}
		logger.SetLogLevel(level)
	}
	
	// Reconfigure connection pool if needed
	if config.MaxConnections != 0 {
		// This would require reinitializing the connection pool
		// For now, we'll just log that this is a placeholder
		logger.Info("Connection pool reconfiguration is a placeholder - not implemented")
	}
	
	// Update buffer pool if buffer sizes changed
	if len(config.BufferSizes) > 0 {
		s.bufferPool = pool.NewMultiBufferPool("pgserver", config.BufferSizes)
	}
	
	// Update profiling if port changed
	if config.ProfilingPort != "" && config.ProfilingPort != s.httpPort {
		s.httpPort = config.ProfilingPort
		if s.profilingService != nil {
			// Stop existing profiling service
			if err := s.profilingService.Stop(); err != nil {
				logger.Error("Error stopping existing profiling service", "error", err)
			}
		}
		// Create new profiling service
		s.profilingService = NewProfilingService(config.ProfilingPort)
	}
	
	return nil
}

// GetConfig returns the current server configuration
func (s *PostgreSQLServer) GetConfig() *ServerConfig {
	// This is a simplified implementation that returns a partial config
	// A full implementation would need to track all configuration values
	return &ServerConfig{
		TCPPort: s.httpPort, // This isn't quite right, but it's a placeholder
		BufferSizes: []int{512, 1024, 2048, 4096, 8192, 16384}, // Default values
	}
}

// ValidateConfig validates the server configuration
func (config *ServerConfig) Validate() error {
	// Add validation logic here
	// For example, check that ports are valid, paths exist, etc.
	
	if config.MaxConnections < 0 {
		return fmt.Errorf("MaxConnections must be non-negative")
	}
	
	if config.MinConnections < 0 {
		return fmt.Errorf("MinConnections must be non-negative")
	}
	
	if config.ConnectionTimeout < 0 {
		return fmt.Errorf("ConnectionTimeout must be non-negative")
	}
	
	// Add more validation as needed
	
	return nil
}