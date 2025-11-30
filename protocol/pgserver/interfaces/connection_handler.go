package interfaces

import (
	"context"
	"net"
	
	"github.com/jackc/pgx/v5/pgproto3"
)

// ConnectionHandlerInterface defines the interface for connection handling
type ConnectionHandlerInterface interface {
	HandleConnection(conn net.Conn) error
	HandleMessage(ctx context.Context, backend *pgproto3.Backend, msg interface{}) (bool, error)
	Close() error
	HealthCheck() error
}