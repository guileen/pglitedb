// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import (
	"net"
)

// ConnectionFactory is an interface for creating network connections
type ConnectionFactory interface {
	CreateConnection() (net.Conn, error)
}