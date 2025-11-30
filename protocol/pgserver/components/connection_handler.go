package components

import (
	"net"

	"github.com/jackc/pgx/v5/pgproto3"
)

// ConnectionHandlerInterface defines the interface for connection handling
type ConnectionHandlerInterface interface {
	HandleConnection(conn net.Conn)
	HandleMessage(backend *pgproto3.Backend, msg interface{}) bool
}