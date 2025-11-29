// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import (
	"context"
	"net"
)

// ConnectionFactory is an interface for creating network connections
type ConnectionFactory interface {
	CreateConnection(ctx context.Context) (net.Conn, error)
}