// Package pool provides connection pool implementations with lifecycle management,
// health checking, metrics collection, and standardized error handling
package pool

import "errors"

var (
	ErrPoolClosed = errors.New("connection pool is closed")
	ErrTimeout    = errors.New("connection request timed out")
)