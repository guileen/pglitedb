package pgserver

import (
	"net/http"
	"sync"
	
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
)

// Ensure PostgreSQLServer implements ServerInterface
var _ interfaces.ServerInterface = &PostgreSQLServer{}

// PostgreSQLServer represents the main PostgreSQL server
type PostgreSQLServer struct {
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool
	
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
	
	// Create components
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
	
	// TODO: Implement TCP listener and server logic
	
	return nil
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	logger.Info("Closing PostgreSQL server", "was_already_closed", s.closed)
	s.closed = true
	
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

// GetProfilingPort returns the profiling port
func (s *PostgreSQLServer) GetProfilingPort() string {
	return s.httpPort
}

// SetProfilingPort sets the profiling port
func (s *PostgreSQLServer) SetProfilingPort(port string) error {
	s.httpPort = port
	return nil
}

// StartTCP starts the server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	// TODO: Implement TCP listener and server logic
	return nil
}

// StartUnix starts the server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socket string) error {
	// TODO: Implement Unix socket listener and server logic
	return nil
}