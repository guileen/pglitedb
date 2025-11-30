package pgserver

import (
	"context"
	"time"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ProfilingConfig holds configuration for profiling
type ProfilingConfig struct {
	Enabled bool
	Port string
	ProfilingInterval time.Duration
}

// StartProfiling starts the profiling service
func (s *PostgreSQLServer) StartProfiling() error {
	// The server manager handles starting the server which includes profiling
	// This method is kept for backward compatibility
	if s.httpPort != "" && s.profilingService != nil {
		logger.Info("Starting profiling service", "port", s.httpPort)
		go func() {
			if err := s.profilingService.Start(); err != nil {
				logger.Error("Failed to start profiling service", "error", err)
			}
		}()
		return nil
	}
	
	logger.Info("Profiling not configured or disabled")
	return nil
}

// StopProfiling stops the profiling service
func (s *PostgreSQLServer) StopProfiling() error {
	if s.profilingService != nil {
		logger.Info("Stopping profiling service")
		if err := s.profilingService.Stop(); err != nil {
			logger.Error("Failed to stop profiling service", "error", err)
			return err
		}
	}
	return nil
}

// IsProfilingEnabled returns whether profiling is enabled
func (s *PostgreSQLServer) IsProfilingEnabled() bool {
	config := s.serverManager.GetConfig(s)
	return config.ProfilingPort != "" && s.profilingService != nil
}

// GetProfilingPort returns the profiling port
func (s *PostgreSQLServer) GetProfilingPort() string {
	config := s.serverManager.GetConfig(s)
	return config.ProfilingPort
}

// SetProfilingPort sets the profiling port and restarts profiling if needed
func (s *PostgreSQLServer) SetProfilingPort(port string) error {
	// Delegate to server manager for setting profiling port
	return s.serverManager.ApplyConfig(s, &config.ServerConfig{ProfilingPort: port})
}

// ProfilingStatus returns the current profiling status
type ProfilingStatus struct {
	Enabled bool
	Port string
	StartTime time.Time
	Error error
}

// GetProfilingStatus returns the current profiling status
func (s *PostgreSQLServer) GetProfilingStatus() *ProfilingStatus {
	status := &ProfilingStatus{
		Enabled: s.httpPort != "" && s.profilingService != nil,
		Port: s.httpPort,
	}
	
	// Add more status information as needed
	return status
}

// CollectProfilingData collects profiling data
func (s *PostgreSQLServer) CollectProfilingData(ctx context.Context) (map[string]interface{}, error) {
	// This is a placeholder for actual profiling data collection
	// In a real implementation, this would collect data from pprof or other profiling tools
	
	data := make(map[string]interface{})
	
	// Add some basic server information
	data["server_status"] = "running"
	data["profiling_enabled"] = s.IsProfilingEnabled()
	if s.IsProfilingEnabled() {
		data["profiling_port"] = s.GetProfilingPort()
	}
	
	// Add more profiling data collection here
	
	return data, nil
}