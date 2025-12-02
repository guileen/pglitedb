package server

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
	"github.com/guileen/pglitedb/protocol/pgserver/components/connection"
	"github.com/guileen/pglitedb/protocol/pgserver/components/listener"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/server"
	"github.com/guileen/pglitedb/protocol/sql"
)

// ServerManager implements the ServerInterface
type ServerManager struct {
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex

	// Component managers
	lifecycleManager *server.LifecycleManager
	networkManager   *server.NetworkManager
	profilingManager *server.ProfilingManager

	// Buffer pool for memory management
	bufferPool *buffer.BufferPoolManager

	// Component references with proper interface types
	connectionHandler interfaces.ConnectionHandlerInterface
	queryProcessor    interfaces.QueryProcessorInterface
	statementManager  interfaces.PreparedStatementManagerInterface
	
	// Dedicated components for listener management and connection acceptance
	listenerManager    interfaces.ListenerManagementInterface
	connectionAcceptor interfaces.ConnectionAcceptanceInterface
	
	// Configuration
	config *config.ServerConfig
}

// NewServerManager creates a new server manager instance
func NewServerManager(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) interfaces.ServerInterface {
	logger.Info("Creating new server manager instance")
	parser := sql.NewPGParser()

	// Validate config first
	lifecycleManager := server.NewLifecycleManager(nil, nil, nil)
	lifecycleManager.ValidateConfig(cfg)

	// Create components with timeout configuration
	queryProcessor := components.NewQueryProcessor(executor, parser, planner)
	statementManager := components.NewPreparedStatementManager(parser)
	connectionHandler := components.NewConnectionHandlerWithTimeout(
		queryProcessor,
		statementManager,
		parser,
		cfg.ConnectionTimeout,
		cfg.IdleTimeout,
		cfg.MaxLifetime,
	)
	bufferPoolManager := buffer.NewBufferPoolManager()

	// Create dedicated components
	listenerManager := listener.NewListenerManagerImpl()
	connectionAcceptor := connection.NewConnectionAcceptorImpl()

	// Create server managers
	networkManager := server.NewNetworkManager(listenerManager, connectionAcceptor, connectionHandler)
	profilingManager := server.NewProfilingManager().WithProfiling(cfg.ProfilingPort)

	// Create profiling service if enabled
	var profilingService interfaces.ProfilingServiceInterface
	if cfg.ProfilingPort != "" {
		profilingService = components.NewProfilingService(cfg.ProfilingPort)
		profilingManager.SetProfilingService(profilingService)
	}

	server := &ServerManager{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		listenerManager:   listenerManager,
		connectionAcceptor: connectionAcceptor,
		lifecycleManager:   server.NewLifecycleManager(listenerManager, connectionAcceptor, connectionHandler),
		networkManager:     networkManager,
		profilingManager:   profilingManager,
		config:             cfg,
	}

	// Set references
	server.lifecycleManager.SetProfilingService(profilingService)

	// Apply configuration
	if err := server.ApplyConfig(cfg); err != nil {
		logger.Error("Failed to apply configuration", "error", err)
	}

	logger.Info("Server manager instance created successfully")
	return server
}

// Start starts the PostgreSQL server on the specified TCP port
func (s *ServerManager) Start(port string) error {
	logger.Info("Starting PostgreSQL server", "port", port, "protocol", "TCP")

	// Start the profiling HTTP server if enabled
	s.profilingManager.StartProfiling()

	// Start TCP listener
	if err := s.StartTCP(port); err != nil {
		return err
	}

	// Block until the server is closed
	// This is needed to keep the server running
	<-s.lifecycleManager.GetCloseChan()

	return nil
}

// GetProfilingPort returns the profiling port
func (s *ServerManager) GetProfilingPort() string {
	return s.profilingManager.GetProfilingPort()
}

// SetProfilingPort sets the profiling port
func (s *ServerManager) SetProfilingPort(port string) error {
	return s.profilingManager.SetProfilingPort(port)
}

// GetListenerAddress returns the address the server is listening on
func (s *ServerManager) GetListenerAddress() net.Addr {
	return s.networkManager.GetListenerAddress()
}

// GetConnectionCount returns the number of active connections
func (s *ServerManager) GetConnectionCount() int {
	return s.lifecycleManager.GetConnectionCount()
}

// Close shuts down the PostgreSQL server
func (s *ServerManager) Close() error {
	return s.lifecycleManager.Close()
}

// IsClosed returns whether the server is closed
func (s *ServerManager) IsClosed() bool {
	return s.lifecycleManager.IsClosed()
}

// StartTCP starts the server on the specified TCP port
func (s *ServerManager) StartTCP(port string) error {
	s.mu.Lock()
	if s.lifecycleManager.IsClosed() {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	return s.networkManager.StartTCP(port)
}

// StartUnix starts the server on the specified Unix socket
func (s *ServerManager) StartUnix(socket string) error {
	s.mu.Lock()
	if s.lifecycleManager.IsClosed() {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	return s.networkManager.StartUnix(socket)
}

// ApplyConfig applies the configuration to the server
func (s *ServerManager) ApplyConfig(cfg *config.ServerConfig) error {
	// Store the config reference
	if cfg == nil {
		return nil
	}
	
	// Update the server's configuration reference
	s.config = cfg
	
	// Reconfigure connection handler with new timeout settings if possible
	if s.connectionHandler != nil {
		// Try to configure the connection handler with new settings
		if configurableHandler, ok := s.connectionHandler.(interface {
			SetTimeouts(connectionTimeout, idleTimeout, maxLifetime time.Duration)
		}); ok {
			configurableHandler.SetTimeouts(cfg.ConnectionTimeout, cfg.IdleTimeout, cfg.MaxLifetime)
		}
		
		// Log the configuration
		logger.Info("Applied server configuration", 
			"max_connections", cfg.MaxConnections,
			"connection_timeout", cfg.ConnectionTimeout,
			"idle_timeout", cfg.IdleTimeout,
			"max_lifetime", cfg.MaxLifetime,
			"profiling_port", cfg.ProfilingPort)
	}
	
	return nil
}

// GetConfig returns the current server configuration
func (s *ServerManager) GetConfig() *config.ServerConfig {
	// Return the actual config if available, otherwise default
	if s.config != nil {
		return s.config
	}
	return config.DefaultServerConfig()
}

// WithProfiling enables profiling on the specified port
func (s *ServerManager) WithProfiling(port string) interfaces.ServerInterface {
	s.profilingManager = s.profilingManager.WithProfiling(port)
	// Create profiling service
	profilingService := components.NewProfilingService(port)
	s.profilingManager.SetProfilingService(profilingService)
	s.lifecycleManager.SetProfilingService(profilingService)

	return s
}