package pgserver

import (
	"fmt"
	"net"
	"sync"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
	"github.com/guileen/pglitedb/protocol/pgserver/components/connection"
	"github.com/guileen/pglitedb/protocol/pgserver/components/listener"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/server"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// PostgreSQLServer represents the main PostgreSQL server
type PostgreSQLServer struct {
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
}

// NewPostgreSQLServer creates a new PostgreSQL server instance
func NewPostgreSQLServer(executor *sql.Executor, planner *sql.Planner) *PostgreSQLServer {
	logger.Info("Creating new PostgreSQL server instance")
	parser := sql.NewPGParser()

	// Create components with default timeouts
	queryProcessor := components.NewQueryProcessor(executor, parser, planner)
	statementManager := components.NewPreparedStatementManager(parser)
	connectionHandler := components.NewConnectionHandler(queryProcessor, statementManager, parser)
	bufferPoolManager := buffer.NewBufferPoolManager()

	// Create dedicated components
	listenerManager := listener.NewListenerManagerImpl()
	connectionAcceptor := connection.NewConnectionAcceptorImpl()

	// Create server managers
	lifecycleManager := server.NewLifecycleManager(listenerManager, connectionAcceptor, connectionHandler)
	networkManager := server.NewNetworkManager(listenerManager, connectionAcceptor, connectionHandler)
	profilingManager := server.NewProfilingManager()

	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		listenerManager:   listenerManager,
		connectionAcceptor: connectionAcceptor,
		lifecycleManager:   lifecycleManager,
		networkManager:     networkManager,
		profilingManager:   profilingManager,
	}

	// Set references in lifecycle manager
	lifecycleManager.SetProfilingService(nil)

	logger.Info("PostgreSQL server instance created successfully")
	return server
}

// NewPostgreSQLServerWithConfig creates a new PostgreSQL server instance with configuration
func NewPostgreSQLServerWithConfig(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) *PostgreSQLServer {
	logger.Info("Creating new PostgreSQL server instance with config")
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

	server := &PostgreSQLServer{
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
	}

	// Set references
	server.lifecycleManager.SetProfilingService(profilingService)

	// Apply configuration
	if err := server.ApplyConfig(cfg); err != nil {
		logger.Error("Failed to apply configuration", "error", err)
	}

	logger.Info("PostgreSQL server instance created successfully with config")
	return server
}

// WithProfiling enables profiling on the specified port
func (s *PostgreSQLServer) WithProfiling(port string) *PostgreSQLServer {
	s.profilingManager = s.profilingManager.WithProfiling(port)
	// Create profiling service
	profilingService := components.NewProfilingService(port)
	s.profilingManager.SetProfilingService(profilingService)
	s.lifecycleManager.SetProfilingService(profilingService)

	return s
}

// Start starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) Start(port string) error {
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
func (s *PostgreSQLServer) GetProfilingPort() string {
	return s.profilingManager.GetProfilingPort()
}

// SetProfilingPort sets the profiling port
func (s *PostgreSQLServer) SetProfilingPort(port string) error {
	return s.profilingManager.SetProfilingPort(port)
}

// GetListenerAddress returns the address the server is listening on
func (s *PostgreSQLServer) GetListenerAddress() net.Addr {
	return s.networkManager.GetListenerAddress()
}

// GetConnectionCount returns the number of active connections
func (s *PostgreSQLServer) GetConnectionCount() int {
	return s.lifecycleManager.GetConnectionCount()
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	return s.lifecycleManager.Close()
}

// StartTCP starts the server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	s.mu.Lock()
	if s.lifecycleManager.IsClosed() {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	return s.networkManager.StartTCP(port)
}

// StartUnix starts the server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socket string) error {
	s.mu.Lock()
	if s.lifecycleManager.IsClosed() {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	return s.networkManager.StartUnix(socket)
}