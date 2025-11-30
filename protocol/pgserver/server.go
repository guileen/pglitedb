package pgserver

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/guileen/pglitedb/network"
	"github.com/guileen/pglitedb/pool"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/logger"
	"github.com/jackc/pgx/v5/pgproto3"
)

// PostgreSQLServer represents the main PostgreSQL server
type PostgreSQLServer struct {
	listener net.Listener
	executor *sql.Executor
	parser   sql.Parser
	planner  *sql.Planner
	mu       sync.Mutex
	closed   bool
	connectionPool *network.ConnectionPool
	
	// HTTP server for profiling endpoints
	httpServer *http.Server
	httpPort   string
	
	// Buffer pools for network I/O
	bufferPool *pool.MultiBufferPool
	
	// Component references
	connectionHandler *ConnectionHandler
	queryProcessor    *QueryProcessor
	statementManager  *PreparedStatementManager
	profilingService  *ProfilingService
}

// NewPostgreSQLServer creates a new PostgreSQL server instance
func NewPostgreSQLServer(executor *sql.Executor, planner *sql.Planner) *PostgreSQLServer {
	logger.Info("Creating new PostgreSQL server instance")
	parser := sql.NewPGParser()
	
	// Create buffer pools for common buffer sizes
	bufferSizes := []int{512, 1024, 2048, 4096, 8192, 16384}
	
	// Create connection pool with optimized configuration
	// Based on historical performance report: Increased default pool sizes by 5x
	poolConfig := network.PoolConfig{
		MaxConnections:         100, // Increased from default
		MinConnections:         20,  // Increased from default
		ConnectionTimeout:      30 * time.Second,
		IdleTimeout:            5 * time.Minute,
		MaxLifetime:            1 * time.Hour,
		MaxIdleConns:           50,  // Increased from default
		HealthCheckPeriod:      1 * time.Minute,
		MetricsEnabled:         true,
		
		// Enable adaptive pooling with aggressive scaling as per historical report
		AdaptivePoolingEnabled: true,
		TargetHitRate:          95.0, // Target high hit rate
		MinHitRateThreshold:    80.0, // Expand when hit rate drops below 80%
		MaxHitRateThreshold:    99.0, // Contract when hit rate is very high
		AdaptationInterval:     30 * time.Second, // Check frequently
		ExpansionFactor:        1.5,  // Expand by 50% when needed
		ContractionFactor:      0.8,  // Contract by 20% when over-provisioned
		MaxAdaptiveConnections: 200,  // Maximum pool size
		MinAdaptiveConnections: 10,   // Minimum pool size
	}
	
	// Create a mock connection factory for the pool
	// In a real implementation, this would create actual database connections
	factory := network.NewMockConnectionFactory(false, 0)
	
	// Create the adaptive connection pool
	adaptivePool := network.NewAdaptiveConnectionPool(poolConfig, factory)
	
	// Create components
	bufferPool := pool.NewMultiBufferPool("pgserver", bufferSizes)
	connectionHandler := NewConnectionHandler(executor, parser, planner, adaptivePool.ConnectionPool)
	queryProcessor := NewQueryProcessor(executor, parser, planner)
	statementManager := NewPreparedStatementManager(parser)
	
	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		bufferPool:        bufferPool,
		connectionPool:    adaptivePool.ConnectionPool,
		connectionHandler: connectionHandler,
		queryProcessor:    queryProcessor,
		statementManager:  statementManager,
		httpPort:          "", // No profiling by default
	}
	
	logger.Info("PostgreSQL server instance created successfully")
	return server
}

// WithProfiling enables profiling on the specified port
func (s *PostgreSQLServer) WithProfiling(port string) *PostgreSQLServer {
	s.httpPort = port
	// Create profiling service
	s.profilingService = NewProfilingService(port)
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
	
	return s.StartTCP(port)
}

// StartTCP starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	logger.Info("Starting PostgreSQL server TCP listener", "port", port)
	
	var err error
	s.listener, err = net.Listen("tcp", ":"+port)
	if err != nil {
		logger.Error("Failed to start TCP listener", "error", err, "port", port)
		return fmt.Errorf("failed to start TCP listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on TCP port", "port", port)
	log.Printf("PostgreSQL server listening on TCP port %s", port)
	
	connectionCount := 0
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("TCP listener closed, exiting accept loop")
				return nil
			}
			
			logger.Error("Failed to accept connection", "error", err)
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		
		connectionCount++
		logger.Debug("Accepted new TCP connection", "connection_count", connectionCount, "remote_addr", conn.RemoteAddr().String())
		
		go s.connectionHandler.HandleConnection(conn)
	}
}

// StartUnix starts the PostgreSQL server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socketPath string) error {
	logger.Info("Starting PostgreSQL server Unix socket listener", "socketPath", socketPath)
	
	// Remove existing socket file if it exists
	if err := os.Remove(socketPath); err != nil && !os.IsNotExist(err) {
		logger.Warn("Failed to remove existing socket file", "error", err, "socketPath", socketPath)
		log.Printf("Warning: failed to remove existing socket file: %v", err)
	}
	
	var err error
	s.listener, err = net.Listen("unix", socketPath)
	if err != nil {
		logger.Error("Failed to start Unix socket listener", "error", err, "socketPath", socketPath)
		return fmt.Errorf("failed to start Unix socket listener: %w", err)
	}
	
	logger.Info("PostgreSQL server listening on Unix socket", "socketPath", socketPath)
	log.Printf("PostgreSQL server listening on Unix socket %s", socketPath)
	
	connectionCount := 0
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			closed := s.closed
			s.mu.Unlock()
			
			if closed {
				logger.Info("Unix socket listener closed, exiting accept loop")
				return nil
			}
			
			logger.Error("Failed to accept connection", "error", err)
			log.Printf("Failed to accept connection: %v", err)
			continue
		}
		
		connectionCount++
		logger.Debug("Accepted new Unix connection", "connection_count", connectionCount, "local_addr", conn.LocalAddr().String())
		
		go s.connectionHandler.HandleConnection(conn)
	}
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	logger.Info("Closing PostgreSQL server", "was_already_closed", s.closed)
	s.closed = true
	
	if s.listener != nil {
		err := s.listener.Close()
		if err != nil {
			logger.Error("Error closing listener", "error", err)
			return err
		}
		logger.Info("PostgreSQL server listener closed successfully")
	}
	
	// Close the connection pool if it exists
	if s.connectionPool != nil {
		err := s.connectionPool.Close()
		if err != nil {
			logger.Error("Error closing connection pool", "error", err)
			return err
		}
		logger.Info("PostgreSQL server connection pool closed successfully")
	}
	
	// Stop the profiling server
	if s.profilingService != nil {
		if err := s.profilingService.Stop(); err != nil {
			logger.Error("Error stopping profiling service", "error", err)
			return err
		}
	}
	
	logger.Info("PostgreSQL server closed successfully")
	return nil
}