package utils

import (
	"errors"
	"fmt"
)

// ErrorHandler provides error handling utilities
type ErrorHandler struct{}

// Standard error types
var (
	ErrNotFound   = errors.New("record not found")
	ErrConflict   = errors.New("conflict")
	ErrValidation = errors.New("validation error")
	ErrInternal   = errors.New("internal error")
	ErrTimeout    = errors.New("timeout")
)

// EngineError represents a structured error with category and details
type EngineError struct {
	Code     string
	Message  string
	Category string
	Err      error
}

func (e *EngineError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("[%s:%s] %s: %v", e.Category, e.Code, e.Message, e.Err)
	}
	return fmt.Sprintf("[%s:%s] %s", e.Category, e.Code, e.Message)
}

func (e *EngineError) Unwrap() error {
	return e.Err
}

// NewErrorHandler creates a new error handler
func NewErrorHandler() *ErrorHandler {
	return &ErrorHandler{}
}

// WrapError wraps an error with additional context
func (eh *ErrorHandler) WrapError(err error, msg string, args ...interface{}) error {
	return fmt.Errorf("%s: %w", fmt.Sprintf(msg, args...), err)
}

// NewEngineError creates a new structured engine error
func (eh *ErrorHandler) NewEngineError(category, code, message string, err error) *EngineError {
	return &EngineError{
		Code:     code,
		Message:  message,
		Category: category,
		Err:      err,
	}
}

// IsNotFoundError checks if an error is a not found error
func (eh *ErrorHandler) IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard not found error
	if errors.Is(err, ErrNotFound) {
		return true
	}
	
	// Check for engine error with not found category
	var engineErr *EngineError
	if errors.As(err, &engineErr) {
		return engineErr.Category == "NOT_FOUND"
	}
	
	// Check for string-based not found errors
	return err.Error() == "record not found" || err.Error() == "not found"
}

// IsConflictError checks if an error is a conflict error
func (eh *ErrorHandler) IsConflictError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard conflict error
	if errors.Is(err, ErrConflict) {
		return true
	}
	
	// Check for engine error with conflict category
	var engineErr *EngineError
	if errors.As(err, &engineErr) {
		return engineErr.Category == "CONFLICT"
	}
	
	// Check for string-based conflict errors
	return err.Error() == "conflict"
}

// IsValidationError checks if an error is a validation error
func (eh *ErrorHandler) IsValidationError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard validation error
	if errors.Is(err, ErrValidation) {
		return true
	}
	
	// Check for engine error with validation category
	var engineErr *EngineError
	if errors.As(err, &engineErr) {
		return engineErr.Category == "VALIDATION"
	}
	
	return false
}

// IsTimeoutError checks if an error is a timeout error
func (eh *ErrorHandler) IsTimeoutError(err error) bool {
	if err == nil {
		return false
	}
	
	// Check for standard timeout error
	if errors.Is(err, ErrTimeout) {
		return true
	}
	
	// Check for engine error with timeout category
	var engineErr *EngineError
	if errors.As(err, &engineErr) {
		return engineErr.Category == "TIMEOUT"
	}
	
	return false
}