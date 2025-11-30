package pgserver

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
	"github.com/guileen/pglitedb/protocol/pgserver/components/connection"
	"github.com/guileen/pglitedb/protocol/pgserver/components/listener"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// PostgreSQLServer represents the main PostgreSQL server
type PostgreSQLServer struct {
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool

	// Connection tracking
	connectionCount int64

	// HTTP server for profiling endpoints
	httpServer *http.Server
	httpPort   string

	// Buffer pool for memory management
	bufferPool *buffer.BufferPoolManager

	// Component references with proper interface types
	connectionHandler interfaces.ConnectionHandlerInterface
	queryProcessor    interfaces.QueryProcessorInterface
	statementManager  interfaces.PreparedStatementManagerInterface
	profilingService  interfaces.ProfilingServiceInterface
	
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

	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		httpPort:          "", // No profiling by default
		listenerManager:   listenerManager,
		connectionAcceptor: connectionAcceptor,
	}

	logger.Info("PostgreSQL server instance created successfully")
	return server
}

// NewPostgreSQLServerWithConfig creates a new PostgreSQL server instance with configuration
func NewPostgreSQLServerWithConfig(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) *PostgreSQLServer {
	logger.Info("Creating new PostgreSQL server instance with config")
	parser := sql.NewPGParser()

	// Validate config first
	ValidateConfig(cfg)

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

	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		httpPort:          cfg.ProfilingPort,
		listenerManager:   listenerManager,
		connectionAcceptor: connectionAcceptor,
	}

	// Apply configuration
	if err := server.ApplyConfig(cfg); err != nil {
		logger.Error("Failed to apply configuration", "error", err)
	}

	logger.Info("PostgreSQL server instance created successfully with config")
	return server
}

// WithProfiling enables profiling on the specified port
func (s *PostgreSQLServer) WithProfiling(port string) *PostgreSQLServer {
	s.httpPort = port
	// Create profiling service
	s.profilingService = components.NewProfilingService(port)

	return s
}

// Start starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) Start(port string) error {
	logger.Info("Starting PostgreSQL server", "port", port, "protocol", "TCP")

	// Start the profiling HTTP server if enabled
	if s.httpPort != "" && s.profilingService != nil {
		go func() {
			if err := s.profilingService.Start(); err != nil {
				logger.Error("Failed to start profiling service", "error", err)
			}
		}()
	}

	// Start TCP listener
	return s.StartTCP(port)
}

// GetProfilingPort returns the profiling port
func (s *PostgreSQLServer) GetProfilingPort() string {
	return s.httpPort
}

// SetProfilingPort sets the profiling port
func (s *PostgreSQLServer) SetProfilingPort(port string) error {
	s.httpPort = port
	return nil
}

// GetListenerAddress returns the address the server is listening on
func (s *PostgreSQLServer) GetListenerAddress() net.Addr {
	return s.listenerManager.GetListenerAddress()
}

// GetConnectionCount returns the number of active connections
func (s *PostgreSQLServer) GetConnectionCount() int {
	// If we have a connection acceptor that tracks connections, use it
	if ca, ok := s.connectionAcceptor.(interface{ GetConnectionCount() int }); ok {
		return ca.GetConnectionCount()
	}
	return int(atomic.LoadInt64(&s.connectionCount))
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logger.Info("Closing PostgreSQL server", "was_already_closed", s.closed)
	if s.closed {
		return nil
	}
	s.closed = true

	// Close the listener through listener manager
	if err := s.listenerManager.CloseListener(); err != nil {
		logger.Error("Error closing listener", "error", err)
		// Continue with other cleanup even if this fails
	}

	// Stop accepting connections
	if s.connectionAcceptor != nil {
		if ca, ok := s.connectionAcceptor.(interface{ StopAcceptingConnections() }); ok {
			ca.StopAcceptingConnections()
		}
	}

	// Stop the profiling server
	if s.profilingService != nil {
		if err := s.profilingService.Stop(); err != nil {
			logger.Error("Error stopping profiling service", "error", err)
			return err
		}
	}

	// Close components
	if s.connectionHandler != nil {
		if err := s.connectionHandler.Close(); err != nil {
			logger.Error("Error closing connection handler", "error", err)
			// Continue closing other components even if this fails
		}
	}

	logger.Info("PostgreSQL server closed successfully")
	return nil
}

// StartTCP starts the server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	// Start TCP listener through listener manager
	if err := s.listenerManager.StartTCP(port); err != nil {
		return err
	}

	listener := s.listenerManager.GetListener()
	logger.Info("PostgreSQL server listening on TCP", "address", listener.Addr().String())

	// Start accepting connections through connection acceptor
	s.connectionAcceptor.StartAcceptingConnections(listener, s.connectionHandler)

	return nil
}

// StartUnix starts the server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socket string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return fmt.Errorf("server is closed")
	}
	s.mu.Unlock()

	// Start Unix socket listener through listener manager
	if err := s.listenerManager.StartUnix(socket); err != nil {
		return err
	}

	listener := s.listenerManager.GetListener()
	logger.Info("PostgreSQL server listening on Unix socket", "socket", listener.Addr().String())

	// Start accepting connections through connection acceptor
	s.connectionAcceptor.StartAcceptingConnections(listener, s.connectionHandler)

	return nil
}