package network

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// NetworkError represents a structured network error
type NetworkError struct {
	Operation string
	Address   string
	Err       error
}

func (ne *NetworkError) Error() string {
	if ne.Address != "" {
		return fmt.Sprintf("network error during %s to %s: %v", ne.Operation, ne.Address, ne.Err)
	}
	return fmt.Sprintf("network error during %s: %v", ne.Operation, ne.Err)
}

func (ne *NetworkError) Unwrap() error {
	return ne.Err
}

// IsNetworkError checks if an error is a network error
func IsNetworkError(err error) bool {
	var target *NetworkError
	return errors.As(err, &target)
}

// NewNetworkError creates a new network error
func NewNetworkError(operation, address string, err error) *NetworkError {
	return &NetworkError{
		Operation: operation,
		Address:   address,
		Err:       err,
	}
}

// IsTimeoutError checks if an error is a timeout error
func IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for connection pool timeout errors
	if IsConnectionPoolError(err) {
		var poolErr *ConnectionPoolError
		if errors.As(err, &poolErr) {
			return poolErr.Op == "acquire_connection"
		}
	}
	
	// Check for standard timeout errors
	return err == context.DeadlineExceeded || 
		err == context.Canceled ||
		strings.Contains(err.Error(), "context deadline exceeded") ||
		strings.Contains(err.Error(), "context canceled")
}

// IsConnectionError checks if an error is a connection error
func IsConnectionError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for connection pool errors
	if IsConnectionPoolError(err) {
		return true
	}
	
	// Check for network errors
	if IsNetworkError(err) {
		return true
	}
	
	// Check for standard connection errors
	errStr := err.Error()
	return strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset by peer") ||
		strings.Contains(errStr, "broken pipe")
}

// WrapError wraps an error with additional context
func WrapError(err error, msg string, args ...interface{}) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(msg, args...), err)
}

// IsNotFoundError checks if an error indicates a resource was not found
func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard not found errors
	return err.Error() == "record not found" || 
		err.Error() == "not found" ||
		err.Error() == "no rows in result set"
}

// IsConflictError checks if an error indicates a conflict
func IsConflictError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard conflict errors
	return err.Error() == "conflict" ||
		err.Error() == "duplicate key value violates unique constraint"
}