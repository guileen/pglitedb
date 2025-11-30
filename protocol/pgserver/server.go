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
	
	// Network listener and connection tracking
	listener        net.Listener
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
	
	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		httpPort:          "", // No profiling by default
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
	
	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		bufferPool:        bufferPoolManager,
		httpPort:          cfg.ProfilingPort,
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
	s.mu.Lock()
	defer s.mu.Unlock()
	
	if s.listener != nil {
		return s.listener.Addr()
	}
	return nil
}

// GetConnectionCount returns the number of active connections
func (s *PostgreSQLServer) GetConnectionCount() int {
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
	
	// Close the listener
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			logger.Error("Error closing listener", "error", err)
			// Continue with other cleanup even if this fails
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
	
	// Create TCP listener
	address := fmt.Sprintf(":%s", port)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		logger.Error("Failed to create TCP listener", "error", err, "address", address)
		return fmt.Errorf("failed to create TCP listener: %w", err)
	}
	
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	
	logger.Info("PostgreSQL server listening on TCP", "address", listener.Addr().String())
	
	// Start accepting connections
	go func() {
		if err := s.acceptConnections(listener); err != nil {
			logger.Error("Error accepting connections", "error", err)
		}
	}()
	
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
	
	// Create Unix socket listener
	listener, err := net.Listen("unix", socket)
	if err != nil {
		logger.Error("Failed to create Unix socket listener", "error", err, "socket", socket)
		return fmt.Errorf("failed to create Unix socket listener: %w", err)
	}
	
	s.mu.Lock()
	s.listener = listener
	s.mu.Unlock()
	
	logger.Info("PostgreSQL server listening on Unix socket", "socket", listener.Addr().String())
	
	// Start accepting connections
	go func() {
		if err := s.acceptConnections(listener); err != nil {
			logger.Error("Error accepting connections", "error", err)
		}
	}()
	
	return nil
}

// acceptConnections handles incoming connections
func (s *PostgreSQLServer) acceptConnections(listener net.Listener) error {
	logger.Info("Starting to accept connections", "address", listener.Addr().String())
	
	for {
		conn, err := listener.Accept()
		if err != nil {
			// Check if server is closing
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("Server is closing, stopping connection acceptance")
				return nil
			}
			
			logger.Error("Failed to accept connection", "error", err)
			continue
		}

		// Increment connection count
		atomic.AddInt64(&s.connectionCount, 1)
		logger.Info("Accepted new connection", "remote_addr", conn.RemoteAddr().String(), "local_addr", conn.LocalAddr().String(), "connection_count", atomic.LoadInt64(&s.connectionCount))

		// Handle connection in a goroutine
		go func() {
			defer func() {
				atomic.AddInt64(&s.connectionCount, -1)
				logger.Info("Connection closed", "remote_addr", conn.RemoteAddr().String(), "connection_count", atomic.LoadInt64(&s.connectionCount))
			}()
			
			if err := s.connectionHandler.HandleConnection(conn); err != nil {
				logger.Error("Error handling connection", "error", err, "remote_addr", conn.RemoteAddr().String())
			}
		}()
	}
}