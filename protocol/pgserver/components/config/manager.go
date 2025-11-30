package config

import (
	"fmt"
	"sync"
	"time"

	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ConfigManager handles server configuration management
type ConfigManager struct {
	serverConfig *config.ServerConfig
	mu           sync.RWMutex
}

// NewConfigManager creates a new configuration manager with default configuration
func NewConfigManager() *ConfigManager {
	return &ConfigManager{
		serverConfig: config.DefaultServerConfig(),
	}
}

// ApplyConfig applies the provided configuration to the server
func (cm *ConfigManager) ApplyConfig(config *config.ServerConfig) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Apply all configuration values
	if config.TCPHost != "" {
		cm.serverConfig.TCPHost = config.TCPHost
	}
	if config.TCPPort != "" {
		cm.serverConfig.TCPPort = config.TCPPort
	}
	if config.UnixSocketPath != "" {
		cm.serverConfig.UnixSocketPath = config.UnixSocketPath
	}

	// Apply connection pool configuration
	if config.MaxConnections > 0 {
		cm.serverConfig.MaxConnections = config.MaxConnections
	}
	if config.MinConnections >= 0 {
		cm.serverConfig.MinConnections = config.MinConnections
	}
	if config.ConnectionTimeout > 0 {
		cm.serverConfig.ConnectionTimeout = config.ConnectionTimeout
	}
	if config.IdleTimeout > 0 {
		cm.serverConfig.IdleTimeout = config.IdleTimeout
	}
	if config.MaxLifetime > 0 {
		cm.serverConfig.MaxLifetime = config.MaxLifetime
	}
	if config.MaxIdleConns >= 0 {
		cm.serverConfig.MaxIdleConns = config.MaxIdleConns
	}
	if config.HealthCheckPeriod > 0 {
		cm.serverConfig.HealthCheckPeriod = config.HealthCheckPeriod
	}

	// Apply adaptive pooling configuration
	cm.serverConfig.AdaptivePoolingEnabled = config.AdaptivePoolingEnabled
	if config.TargetHitRate > 0 {
		cm.serverConfig.TargetHitRate = config.TargetHitRate
	}
	if config.MinHitRateThreshold > 0 {
		cm.serverConfig.MinHitRateThreshold = config.MinHitRateThreshold
	}
	if config.MaxHitRateThreshold > 0 {
		cm.serverConfig.MaxHitRateThreshold = config.MaxHitRateThreshold
	}
	if config.AdaptationInterval > 0 {
		cm.serverConfig.AdaptationInterval = config.AdaptationInterval
	}
	if config.ExpansionFactor > 0 {
		cm.serverConfig.ExpansionFactor = config.ExpansionFactor
	}
	if config.ContractionFactor > 0 {
		cm.serverConfig.ContractionFactor = config.ContractionFactor
	}
	if config.MaxAdaptiveConnections > 0 {
		cm.serverConfig.MaxAdaptiveConnections = config.MaxAdaptiveConnections
	}
	if config.MinAdaptiveConnections > 0 {
		cm.serverConfig.MinAdaptiveConnections = config.MinAdaptiveConnections
	}

	// Apply buffer pool configuration
	if len(config.BufferSizes) > 0 {
		cm.serverConfig.BufferSizes = config.BufferSizes
	}

	// Apply profiling configuration
	if config.ProfilingPort != "" {
		cm.serverConfig.ProfilingPort = config.ProfilingPort
	}

	// Apply logging configuration
	if config.LogLevel != "" {
		cm.serverConfig.LogLevel = config.LogLevel
	}

	return nil
}

// GetConfig returns the current server configuration
func (cm *ConfigManager) GetConfig() *config.ServerConfig {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	
	// Return a copy to prevent external modification
	configCopy := *cm.serverConfig
	return &configCopy
}

// ValidateConfig validates the provided configuration
func (cm *ConfigManager) ValidateConfig(config *config.ServerConfig) error {
	// Add validation logic here
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

// UpdateNetworkConfig updates network-related configuration
func (cm *ConfigManager) UpdateNetworkConfig(tcpHost, tcpPort, unixSocketPath string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if tcpHost != "" {
		cm.serverConfig.TCPHost = tcpHost
	}
	if tcpPort != "" {
		cm.serverConfig.TCPPort = tcpPort
	}
	if unixSocketPath != "" {
		cm.serverConfig.UnixSocketPath = unixSocketPath
	}

	return nil
}

// UpdateConnectionPoolConfig updates connection pool configuration
func (cm *ConfigManager) UpdateConnectionPoolConfig(maxConn, minConn int, connTimeout, idleTimeout, maxLifetime int64) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if maxConn > 0 {
		cm.serverConfig.MaxConnections = maxConn
	}
	if minConn >= 0 {
		cm.serverConfig.MinConnections = minConn
	}
	if connTimeout > 0 {
		cm.serverConfig.ConnectionTimeout = time.Duration(connTimeout) * time.Second
	}
	if idleTimeout > 0 {
		cm.serverConfig.IdleTimeout = time.Duration(idleTimeout) * time.Second
	}
	if maxLifetime > 0 {
		cm.serverConfig.MaxLifetime = time.Duration(maxLifetime) * time.Second
	}

	return nil
}

// UpdateBufferConfig updates buffer pool configuration
func (cm *ConfigManager) UpdateBufferConfig(bufferSizes []int) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if len(bufferSizes) > 0 {
		cm.serverConfig.BufferSizes = bufferSizes
	}

	return nil
}

// UpdateProfilingConfig updates profiling configuration
func (cm *ConfigManager) UpdateProfilingConfig(port string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if port != "" {
		cm.serverConfig.ProfilingPort = port
	}

	return nil
}

// UpdateLoggingConfig updates logging configuration
func (cm *ConfigManager) UpdateLoggingConfig(level string) error {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if level != "" {
		cm.serverConfig.LogLevel = level
	}

	return nil
}

// HealthCheck performs a health check on the configuration manager
func (cm *ConfigManager) HealthCheck() error {
	// Simple health check implementation
	return nil
}