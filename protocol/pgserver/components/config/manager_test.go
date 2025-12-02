package config

import (
	"testing"
	"time"
	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

func TestConfigManager_NewConfigManager(t *testing.T) {
	cm := NewConfigManager()
	assert.NotNil(t, cm)
	assert.NotNil(t, cm.GetConfig())
}

func TestConfigManager_ApplyConfig(t *testing.T) {
	cm := NewConfigManager()
	
	// Create a test configuration
	testConfig := &config.ServerConfig{
		TCPHost:      "localhost",
		TCPPort:      "5432",
		MaxConnections: 100,
		LogLevel:     "debug",
	}
	
	err := cm.ApplyConfig(testConfig)
	assert.NoError(t, err)
	
	// Verify the configuration was applied
	appliedConfig := cm.GetConfig()
	assert.Equal(t, "localhost", appliedConfig.TCPHost)
	assert.Equal(t, "5432", appliedConfig.TCPPort)
	assert.Equal(t, 100, appliedConfig.MaxConnections)
	assert.Equal(t, "debug", appliedConfig.LogLevel)
}

func TestConfigManager_GetConfig_ReturnsCopy(t *testing.T) {
	cm := NewConfigManager()
	
	// Get config and modify it
	config1 := cm.GetConfig()
	config1.TCPHost = "modified"
	
	// Get config again and verify it wasn't affected by the modification
	config2 := cm.GetConfig()
	assert.NotEqual(t, "modified", config2.TCPHost)
}

func TestConfigManager_ValidateConfig(t *testing.T) {
	cm := NewConfigManager()
	
	// Test valid configuration
	validConfig := &config.ServerConfig{
		MaxConnections: 100,
		MinConnections: 10,
	}
	err := cm.ValidateConfig(validConfig)
	assert.NoError(t, err)
	
	// Test invalid configurations
	invalidConfigs := []*config.ServerConfig{
		{MaxConnections: -1},
		{MinConnections: -1},
		{ConnectionTimeout: -1},
	}
	
	for _, invalidConfig := range invalidConfigs {
		err := cm.ValidateConfig(invalidConfig)
		assert.Error(t, err)
	}
}

func TestConfigManager_UpdateNetworkConfig(t *testing.T) {
	cm := NewConfigManager()
	
	err := cm.UpdateNetworkConfig("localhost", "5432", "/tmp/socket")
	assert.NoError(t, err)
	
	config := cm.GetConfig()
	assert.Equal(t, "localhost", config.TCPHost)
	assert.Equal(t, "5432", config.TCPPort)
	assert.Equal(t, "/tmp/socket", config.UnixSocketPath)
	
	// Test partial updates
	err = cm.UpdateNetworkConfig("newhost", "", "")
	assert.NoError(t, err)
	
	config = cm.GetConfig()
	assert.Equal(t, "newhost", config.TCPHost)
	assert.Equal(t, "5432", config.TCPPort) // Should remain unchanged
}

func TestConfigManager_UpdateConnectionPoolConfig(t *testing.T) {
	cm := NewConfigManager()
	
	maxConn := 200
	minConn := 20
	connTimeout := int64(30)
	idleTimeout := int64(60)
	maxLifetime := int64(3600)
	
	err := cm.UpdateConnectionPoolConfig(maxConn, minConn, connTimeout, idleTimeout, maxLifetime)
	assert.NoError(t, err)
	
	config := cm.GetConfig()
	assert.Equal(t, maxConn, config.MaxConnections)
	assert.Equal(t, minConn, config.MinConnections)
	assert.Equal(t, time.Duration(connTimeout)*time.Second, config.ConnectionTimeout)
	assert.Equal(t, time.Duration(idleTimeout)*time.Second, config.IdleTimeout)
	assert.Equal(t, time.Duration(maxLifetime)*time.Second, config.MaxLifetime)
}

func TestConfigManager_UpdateBufferConfig(t *testing.T) {
	cm := NewConfigManager()
	
	bufferSizes := []int{1024, 2048, 4096}
	err := cm.UpdateBufferConfig(bufferSizes)
	assert.NoError(t, err)
	
	config := cm.GetConfig()
	assert.Equal(t, bufferSizes, config.BufferSizes)
}

func TestConfigManager_UpdateProfilingConfig(t *testing.T) {
	cm := NewConfigManager()
	
	err := cm.UpdateProfilingConfig(":6060")
	assert.NoError(t, err)
	
	config := cm.GetConfig()
	assert.Equal(t, ":6060", config.ProfilingPort)
}

func TestConfigManager_UpdateLoggingConfig(t *testing.T) {
	cm := NewConfigManager()
	
	err := cm.UpdateLoggingConfig("info")
	assert.NoError(t, err)
	
	config := cm.GetConfig()
	assert.Equal(t, "info", config.LogLevel)
}

func TestConfigManager_HealthCheck(t *testing.T) {
	cm := NewConfigManager()
	
	err := cm.HealthCheck()
	assert.NoError(t, err)
}