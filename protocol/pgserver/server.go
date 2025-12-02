package pgserver

import (
	"net"

	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
	server "github.com/guileen/pglitedb/protocol/pgserver/internal/servermanager"
	"github.com/guileen/pglitedb/protocol/sql"
)

// PostgreSQLServer represents the main PostgreSQL server
type PostgreSQLServer struct {
	serverManager interfaces.ServerInterface
}

// NewPostgreSQLServer creates a new PostgreSQL server instance
func NewPostgreSQLServer(executor *sql.Executor, planner *sql.Planner) *PostgreSQLServer {
	// Create default configuration
	defaultConfig := config.DefaultServerConfig()
	return NewPostgreSQLServerWithConfig(executor, planner, defaultConfig)
}

// NewPostgreSQLServerWithConfig creates a new PostgreSQL server instance with configuration
func NewPostgreSQLServerWithConfig(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) *PostgreSQLServer {
	manager := server.NewServerManager(executor, planner, cfg)
	
	return &PostgreSQLServer{
		serverManager: manager,
	}
}

// Start starts the PostgreSQL server on the specified TCP port
func (s *PostgreSQLServer) Start(port string) error {
	return s.serverManager.Start(port)
}

// GetProfilingPort returns the profiling port
func (s *PostgreSQLServer) GetProfilingPort() string {
	return s.serverManager.GetProfilingPort()
}

// SetProfilingPort sets the profiling port
func (s *PostgreSQLServer) SetProfilingPort(port string) error {
	return s.serverManager.SetProfilingPort(port)
}

// GetListenerAddress returns the address the server is listening on
func (s *PostgreSQLServer) GetListenerAddress() net.Addr {
	return s.serverManager.GetListenerAddress()
}

// GetConnectionCount returns the number of active connections
func (s *PostgreSQLServer) GetConnectionCount() int {
	return s.serverManager.GetConnectionCount()
}

// Close shuts down the PostgreSQL server
func (s *PostgreSQLServer) Close() error {
	return s.serverManager.Close()
}

// IsClosed returns whether the server is closed
func (s *PostgreSQLServer) IsClosed() bool {
	return s.serverManager.IsClosed()
}

// StartTCP starts the server on the specified TCP port
func (s *PostgreSQLServer) StartTCP(port string) error {
	return s.serverManager.StartTCP(port)
}

// StartUnix starts the server on the specified Unix socket
func (s *PostgreSQLServer) StartUnix(socket string) error {
	return s.serverManager.StartUnix(socket)
}

// ApplyConfig applies the configuration to the server
func (s *PostgreSQLServer) ApplyConfig(cfg *config.ServerConfig) error {
	return s.serverManager.ApplyConfig(cfg)
}

// GetConfig returns the current server configuration
func (s *PostgreSQLServer) GetConfig() *config.ServerConfig {
	return s.serverManager.GetConfig()
}

// WithProfiling enables profiling on the specified port
func (s *PostgreSQLServer) WithProfiling(port string) interfaces.ServerInterface {
	s.serverManager = s.serverManager.WithProfiling(port)
	return s
}