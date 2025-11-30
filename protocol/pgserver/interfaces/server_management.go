package interfaces

import (
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ServerManagementInterface defines the interface for server management operations
type ServerManagementInterface interface {
	ApplyConfig(server ServerInterface, config *config.ServerConfig) error
	GetConfig(server ServerInterface) *config.ServerConfig
	ValidateConfig(config *config.ServerConfig) error
	Start(server ServerInterface, port string) error
	Close(server ServerInterface) error
	IsClosed() bool
	HealthCheck() error
}

// ServerInterface defines the interface that the PostgreSQLServer must implement
type ServerInterface interface {
	GetProfilingPort() string
	SetProfilingPort(port string) error
	StartTCP(port string) error
	Close() error
}