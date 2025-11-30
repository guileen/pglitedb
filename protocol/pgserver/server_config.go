package pgserver

import (
	"github.com/guileen/pglitedb/protocol/pgserver/components/server"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ApplyConfig applies the configuration to the server
func (s *PostgreSQLServer) ApplyConfig(config *config.ServerConfig) error {
	return s.serverManager.ApplyConfig(s, config)
}

// GetConfig returns the current server configuration
func (s *PostgreSQLServer) GetConfig() *config.ServerConfig {
	return s.serverManager.GetConfig(s)
}

// ValidateConfig validates the server configuration
func ValidateConfig(config *config.ServerConfig) error {
	// Use server manager for validation
	manager := server.NewServerManager()
	return manager.ValidateConfig(config)
}