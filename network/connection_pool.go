// Package network provides networking utilities for PostgreSQL connections
package network

import (
	"context"
	"net"
)

// ConnectionFactory is an interface for creating network connections
type ConnectionFactory interface {
	CreateConnection(ctx context.Context) (net.Conn, error)
}