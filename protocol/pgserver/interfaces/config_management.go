package interfaces

import (
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ConfigManagementInterface defines the interface for configuration management operations
type ConfigManagementInterface interface {
	ApplyConfig(config *config.ServerConfig) error
	GetConfig() *config.ServerConfig
	ValidateConfig(config *config.ServerConfig) error
	UpdateNetworkConfig(tcpHost, tcpPort, unixSocketPath string) error
	UpdateConnectionPoolConfig(maxConn, minConn int, connTimeout, idleTimeout, maxLifetime int64) error
	UpdateBufferConfig(bufferSizes []int) error
	UpdateProfilingConfig(port string) error
	UpdateLoggingConfig(level string) error
	HealthCheck() error
}