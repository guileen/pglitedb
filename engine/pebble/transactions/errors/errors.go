// Package errors provides standardized error types for transaction operations.
package errors

import (
	"fmt"
	dbTypes "github.com/guileen/pglitedb/types"
)

// Error constants for transaction operations
var (
	// ErrClosed is returned when attempting to operate on a closed transaction
	ErrClosed = &TransactionError{code: "closed", msg: "transaction is closed"}
	
	// ErrInvalidIsolation is returned when trying to set an invalid isolation level
	ErrInvalidIsolation = &TransactionError{code: "invalid_isolation", msg: "invalid isolation level"}
	
	// ErrConflict is returned when a transaction conflict is detected
	ErrConflict = &TransactionError{code: "conflict", msg: "transaction conflict detected"}
	
	// ErrIteratorFailure is returned when iterator operations fail
	ErrIteratorFailure = &TransactionError{code: "iterator_failure", msg: "iterator operation failed"}
	
	// ErrBatchOperation is returned when batch operations fail
	ErrBatchOperation = &TransactionError{code: "batch_operation", msg: "batch operation failed"}
	
	// ErrRowNotFound is returned when a requested row cannot be found
	// This maintains backward compatibility with dbTypes.ErrRecordNotFound
	ErrRowNotFound = &compatibleError{
		TransactionError: &TransactionError{code: "row_not_found", msg: "row not found"},
		compatible:       dbTypes.ErrRecordNotFound,
	}
	
	// ErrEncodingFailure is returned when encoding operations fail
	ErrEncodingFailure = &TransactionError{code: "encoding_failure", msg: "data encoding failed"}
	
	// ErrDecodingFailure is returned when decoding operations fail
	ErrDecodingFailure = &TransactionError{code: "decoding_failure", msg: "data decoding failed"}
	
	// ErrGenerateID is returned when row ID generation fails
	ErrGenerateID = &TransactionError{code: "generate_id_failure", msg: "failed to generate row ID"}
	
	// ErrIteratorCreation is returned when iterator creation fails
	ErrIteratorCreation = &TransactionError{code: "iterator_creation", msg: "failed to create iterator"}
)

// compatibleError wraps TransactionError and maintains compatibility with legacy errors
type compatibleError struct {
	*TransactionError
	compatible error
}

// Error implements the error interface
func (e *compatibleError) Error() string {
	return e.TransactionError.Error()
}

// Unwrap returns the wrapped error
func (e *compatibleError) Unwrap() error {
	return e.TransactionError
}

// Is checks if the error matches the target
func (e *compatibleError) Is(target error) bool {
	// Check if target is the compatible error
	if target == e.compatible {
		return true
	}
	// Delegate to TransactionError.Is
	return e.TransactionError.Is(target)
}

// TransactionError represents a transaction-specific error
type TransactionError struct {
	code string
	msg  string
	err  error // wrapped error
}

// Error implements the error interface
func (e *TransactionError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.msg, e.err)
	}
	return e.msg
}

// Code returns the error code
func (e *TransactionError) Code() string {
	return e.code
}

// Unwrap returns the wrapped error
func (e *TransactionError) Unwrap() error {
	return e.err
}

// Is checks if the error matches the target
func (e *TransactionError) Is(target error) bool {
	if t, ok := target.(*TransactionError); ok {
		return e.code == t.code
	}
	return false
}

// New creates a new TransactionError with a formatted message
func New(code, format string, args ...interface{}) *TransactionError {
	return &TransactionError{
		code: code,
		msg:  fmt.Sprintf(format, args...),
	}
}

// Wrap wraps an existing error with a transaction error
func Wrap(err error, code, format string, args ...interface{}) *TransactionError {
	return &TransactionError{
		code: code,
		msg:  fmt.Sprintf(format, args...),
		err:  err,
	}
}

// IsTransactionError checks if an error is a TransactionError
func IsTransactionError(err error) bool {
	_, ok := err.(*TransactionError)
	return ok
}

// IsClosedError checks if an error indicates a closed transaction
func IsClosedError(err error) bool {
	return Is(err, ErrClosed)
}

// IsConflictError checks if an error indicates a transaction conflict
func IsConflictError(err error) bool {
	return Is(err, ErrConflict)
}

// IsRowNotFoundError checks if an error indicates a row not found
func IsRowNotFoundError(err error) bool {
	return Is(err, ErrRowNotFound)
}

// Is implements the errors.Is functionality for TransactionError
func Is(err, target error) bool {
	if err == nil {
		return err == target
	}
	
	// Check direct equality first
	if err == target {
		return true
	}
	
	// Check if err implements Is method
	if e, ok := err.(interface{ Is(error) bool }); ok {
		return e.Is(target)
	}
	
	// Check unwrapped errors
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return Is(e.Unwrap(), target)
	}
	
	return false
}