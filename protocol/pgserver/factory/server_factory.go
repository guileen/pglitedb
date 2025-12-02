package factory

import (
	"github.com/guileen/pglitedb/protocol/pgserver"
	"github.com/guileen/pglitedb/protocol/pgserver/config"
	"github.com/guileen/pglitedb/protocol/pgserver/interfaces"
	"github.com/guileen/pglitedb/protocol/sql"
)

// ServerFactoryInterface defines the interface for creating PostgreSQL servers
type ServerFactoryInterface interface {
	CreateServer(executor *sql.Executor, planner *sql.Planner) interfaces.ServerInterface
	CreateServerWithConfig(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) interfaces.ServerInterface
}

// ServerFactory implements the ServerFactoryInterface
type ServerFactory struct{}

// NewServerFactory creates a new server factory
func NewServerFactory() ServerFactoryInterface {
	return &ServerFactory{}
}

// CreateServer creates a new PostgreSQL server with default configuration
func (f *ServerFactory) CreateServer(executor *sql.Executor, planner *sql.Planner) interfaces.ServerInterface {
	return pgserver.NewPostgreSQLServer(executor, planner)
}

// CreateServerWithConfig creates a new PostgreSQL server with the specified configuration
func (f *ServerFactory) CreateServerWithConfig(executor *sql.Executor, planner *sql.Planner, cfg *config.ServerConfig) interfaces.ServerInterface {
	return pgserver.NewPostgreSQLServerWithConfig(executor, planner, cfg)
}