package network

import (
	"errors"
	"fmt"
)

// ConnectionPoolError represents errors specific to connection pool operations
type ConnectionPoolError struct {
	Op  string
	Err error
}

func (e *ConnectionPoolError) Error() string {
	return fmt.Sprintf("connection pool error during %s: %v", e.Op, e.Err)
}

func (e *ConnectionPoolError) Unwrap() error {
	return e.Err
}

// IsConnectionPoolError checks if an error is a connection pool error
func IsConnectionPoolError(err error) bool {
	var target *ConnectionPoolError
	return errors.As(err, &target)
}

var (
	ErrPoolClosed = errors.New("connection pool is closed")
	ErrTimeout    = errors.New("connection request timed out")
)