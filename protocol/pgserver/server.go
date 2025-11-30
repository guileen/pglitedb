package pgserver

import (
	"net/http"
	"sync"
	"time"

	"github.com/guileen/pglitedb/network"
	"github.com/guileen/pglitedb/protocol/sql"
	"github.com/guileen/pglitedb/logger"
	"github.com/guileen/pglitedb/protocol/pgserver/components/server"
	"github.com/guileen/pglitedb/protocol/pgserver/components/connection"
	"github.com/guileen/pglitedb/protocol/pgserver/components/buffer"
	"github.com/guileen/pglitedb/protocol/pgserver/components/config"
	"github.com/guileen/pglitedb/protocol/pgserver/components/listener"
	"github.com/guileen/pglitedb/protocol/pgserver/internal/components"
)

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
	
	// Component managers
	serverManager         *server.ServerManager
	listenerManager       *listener.ListenerManager
	configManager         *config.ConfigManager
	connectionPoolManager *connection.ConnectionPoolManager
	bufferPoolManager     *buffer.BufferPoolManager
	
	// Component references
	connectionHandler interface{} // ConnectionHandlerInterface
	queryProcessor    interface{} // QueryProcessorInterface
	statementManager  interface{} // PreparedStatementManagerInterface
	profilingService  interface{} // ProfilingServiceInterface
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
	
	// Create components
	queryProcessor := components.NewQueryProcessor(executor, parser, planner)
	statementManager := components.NewPreparedStatementManager(parser)
	connectionHandler := components.NewConnectionHandler(queryProcessor, statementManager, parser)
	
	// Create component managers
	serverManager := server.NewServerManager()
	listenerManager := listener.NewListenerManager()
	configManager := config.NewConfigManager()
	connectionPoolManager := connection.NewConnectionPoolManager()
	bufferPoolManager := buffer.NewBufferPoolManager()
	
	// Initialize buffer pool
	bufferPoolManager.InitializeBufferPool("pgserver", bufferSizes)
	
	// Initialize connection pool
	connectionPoolManager.InitializePool(poolConfig, factory)
	
	server := &PostgreSQLServer{
		executor:              executor,
		parser:                parser,
		planner:               planner,
		serverManager:         serverManager,
		listenerManager:       listenerManager,
		configManager:         configManager,
		connectionPoolManager: connectionPoolManager,
		bufferPoolManager:     bufferPoolManager,
		connectionHandler:     connectionHandler,
		queryProcessor:        queryProcessor,
		statementManager:      statementManager,
		httpPort:              "", // No profiling by default
	}
	
	logger.Info("PostgreSQL server instance created successfully")
	return server
}

// WithProfiling enables profiling on the specified port
func (s *PostgreSQLServer) WithProfiling(port string) *PostgreSQLServer {
	s.httpPort = port
	// Create profiling service
	s.profilingService = components.NewProfilingService(port)
	
	// Update the server configuration with the profiling port
	s.configManager.UpdateProfilingConfig(port)
	
	return s
}

// Start starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) Start(port string) error {
	logger.Info("Starting PostgreSQL server", "port", port, "protocol", "TCP")
	
	// Start the profiling HTTP server if enabled
	if s.httpPort != "" && s.profilingService != nil {
		// Type assert to the expected interface and call Start
		if profiler, ok := s.profilingService.(interface {
			Start() error
		}); ok {
			go func() {
				if err := profiler.Start(); err != nil {
					logger.Error("Failed to start profiling service", "error", err)
				}
			}()
		}
	}
	
	// Start TCP listener using listener manager
	if err := s.listenerManager.StartTCP(port); err != nil {
		return err
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
	
	// Close the listener using listener manager
	if err := s.listenerManager.CloseListener(); err != nil {
		return err
	}
	
	// Close the connection pool using connection pool manager
	if err := s.connectionPoolManager.ClosePool(); err != nil {
		return err
	}
	
	// Stop the profiling server
	if s.profilingService != nil {
		// Type assert to the expected interface and call Stop
		if profiler, ok := s.profilingService.(interface {
			Stop() error
		}); ok {
			if err := profiler.Stop(); err != nil {
				logger.Error("Error stopping profiling service", "error", err)
				return err
			}
		}
	}
	
	// Close components
	// Note: We're not calling Close() on connectionHandler as it's not part of the interface
	// In a real implementation, we might need to type assert or add Close to the interface
	
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
	return s.listenerManager.StartTCP(port)
}