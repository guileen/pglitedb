package pgserver

import (
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/guileen/pglitedb/network"
	"github.com/guileen/pglitedb/pool"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/components/server"
	"github.com/guileen/pglitedb/protocol/pgserver/components/connection"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
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
	
	// Component managers
	serverManager      *server.ServerManager
	connectionManager  *connection.ConnectionManager
	bufferPoolManager  *buffer.BufferPoolManager
	
	// Component references
	connectionHandler ConnectionHandlerInterface
	queryProcessor    QueryProcessorInterface
	statementManager  PreparedStatementManagerInterface
	profilingService  ProfilingServiceInterface
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
	queryProcessor := NewQueryProcessor(executor, parser, planner)
	statementManager := NewPreparedStatementManager(parser)
	connectionHandler := NewConnectionHandler(queryProcessor, statementManager, parser)
	
	// Create component managers
	serverManager := server.NewServerManager()
	connectionManager := connection.NewConnectionManager(adaptivePool.ConnectionPool)
	bufferPoolManager := buffer.NewBufferPoolManager(bufferPool)
	
	server := &PostgreSQLServer{
		executor:          executor,
		parser:            parser,
		planner:           planner,
		bufferPool:        bufferPool,
		connectionPool:    adaptivePool.ConnectionPool,
		serverManager:     serverManager,
		connectionManager: connectionManager,
		bufferPoolManager: bufferPoolManager,
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
	
	// Update the server configuration with the profiling port
	config := s.serverManager.GetConfig(s)
	config.ProfilingPort = port
	// In a real implementation, we would apply the config here
	
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
	
	// Use server manager to start the server
	return s.serverManager.Start(s, port)
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	
	logger.Info("Closing PostgreSQL server", "was_already_closed", s.closed)
	s.closed = true
	
	// Close the listener using connection manager
	if err := s.connectionManager.CloseListener(); err != nil {
		return err
	}
	
	// Close the connection pool using connection manager
	if err := s.connectionManager.CloseConnectionPool(); err != nil {
		return err
	}
	
	// Stop the profiling server
	if s.profilingService != nil {
		if err := s.profilingService.Stop(); err != nil {
			logger.Error("Error stopping profiling service", "error", err)
			return err
		}
	}
	
	// Close components
	// Note: We're not calling Close() on connectionHandler as it's not part of the interface
	// In a real implementation, we might need to type assert or add Close to the interface
	
	logger.Info("PostgreSQL server closed successfully")
	return nil
}