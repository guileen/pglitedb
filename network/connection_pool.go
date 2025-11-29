// Package network provides networking utilities for PostgreSQL connections
package network

import (
	"net"
)

// ConnectionFactory is an interface for creating network connections
type ConnectionFactory interface {
	CreateConnection() (net.Conn, error)
}