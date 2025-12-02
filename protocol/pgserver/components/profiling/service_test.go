package profiling

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestService_NewService(t *testing.T) {
	service := NewService("8080")
	assert.NotNil(t, service)
	
	// Test with empty port (disabled)
	disabledService := NewService("")
	assert.NotNil(t, disabledService)
}

func TestService_GetStatus(t *testing.T) {
	service := NewService("8080").(*Service)
	
	status := service.GetStatus()
	assert.NotNil(t, status)
	
	// Check enabled status
	enabled, ok := status["enabled"]
	assert.True(t, ok)
	assert.Equal(t, true, enabled)
	
	// Check port
	port, ok := status["port"]
	assert.True(t, ok)
	assert.Equal(t, "8080", port)
	
	// Check start time
	startTime, ok := status["start_time"]
	assert.True(t, ok)
	assert.NotNil(t, startTime)
	
	// Check uptime
	uptime, ok := status["uptime"]
	assert.True(t, ok)
	assert.NotEmpty(t, uptime)
}

func TestService_GetStatus_Disabled(t *testing.T) {
	service := NewService("").(*Service)
	
	status := service.GetStatus()
	assert.NotNil(t, status)
	
	// Check enabled status
	enabled, ok := status["enabled"]
	assert.True(t, ok)
	assert.Equal(t, false, enabled)
	
	// Check port
	port, ok := status["port"]
	assert.True(t, ok)
	assert.Equal(t, "", port)
}

func TestService_HealthCheck(t *testing.T) {
	service := NewService("8080")
	
	// Health check should not error
	err := service.HealthCheck()
	assert.NoError(t, err)
}

func TestService_CollectData(t *testing.T) {
	service := NewService("8080")
	
	ctx := context.Background()
	data, err := service.CollectData(ctx)
	require.NoError(t, err)
	assert.NotNil(t, data)
	
	// Should contain status information
	status, ok := data["status"]
	assert.True(t, ok)
	assert.NotNil(t, status)
}

func TestService_StartStop_Disabled(t *testing.T) {
	// Test service with no port (should be disabled)
	service := NewService("")
	
	// Starting disabled service should not error
	err := service.Start()
	assert.NoError(t, err)
	
	// Stopping disabled service should not error
	err = service.Stop()
	assert.NoError(t, err)
}

func TestService_Lifecycle(t *testing.T) {
	// Note: We can't actually start the HTTP server in tests as it would conflict
	// But we can test the lifecycle methods
	
	service := NewService("0").(*Service) // Use port 0 to get an available port
	
	// Test that we can call Start and Stop without panicking
	// In a real scenario, we'd test actual HTTP functionality
	
	// Start in a goroutine so it doesn't block
	go func() {
		_ = service.Start()
	}()
	
	// Give it a moment to start
	time.Sleep(10 * time.Millisecond)
	
	// Stop the service
	err := service.Stop()
	assert.NoError(t, err)
}