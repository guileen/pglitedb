package interfaces

import (
	"net"
	
	"github.com/guileen/pglitedb/protocol/pgserver/config"
)

// ServerInterface defines the interface for the main PostgreSQL server
type ServerInterface interface {
	// Lifecycle methods
	Start(port string) error
	Close() error
	IsClosed() bool
	
	// Network methods
	StartTCP(port string) error
	StartUnix(socket string) error
	GetListenerAddress() net.Addr
	
	// Configuration methods
	ApplyConfig(cfg *config.ServerConfig) error
	GetConfig() *config.ServerConfig
	
	// Monitoring methods
	GetConnectionCount() int
	GetProfilingPort() string
	SetProfilingPort(port string) error
	
	// Profiling methods
	WithProfiling(port string) ServerInterface
}