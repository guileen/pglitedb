package pgserver

import (
	"time"
	
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
)

// ApplyConfig applies the configuration to the server
func (s *PostgreSQLServer) ApplyConfig(cfg *config.ServerConfig) error {
	// Store the config reference or apply specific settings
	if cfg == nil {
		return nil
	}
	
	// Reconfigure connection handler with new timeout settings
	if s.connectionHandler != nil {
		// Type assert to check if it's our connection handler implementation
		if _, ok := s.connectionHandler.(*components.ConnectionHandler); ok {
			// Note: In a production system, we would need a method to update timeouts
			// For now, we'll log the configuration
			logger.Info("Applied server configuration", 
				"max_connections", cfg.MaxConnections,
				"connection_timeout", cfg.ConnectionTimeout,
				"idle_timeout", cfg.IdleTimeout,
				"max_lifetime", cfg.MaxLifetime)
		}
	}
	
	return nil
}

// GetConfig returns the current server configuration
func (s *PostgreSQLServer) GetConfig() *config.ServerConfig {
	// Return default config for now
	// In a full implementation, this would return the actual config
	return config.DefaultServerConfig()
}

// ValidateConfig validates the server configuration
func ValidateConfig(cfg *config.ServerConfig) error {
	// Basic validation
	if cfg.MaxConnections <= 0 {
		cfg.MaxConnections = 100 // default
	}
	
	if cfg.ConnectionTimeout <= 0 {
		cfg.ConnectionTimeout = 30 * time.Second // default
	}
	
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = 5 * time.Minute // default
	}
	
	if cfg.MaxLifetime <= 0 {
		cfg.MaxLifetime = 1 * time.Hour // default
	}
	
	return nil
}