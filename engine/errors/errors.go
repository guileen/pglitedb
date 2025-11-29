// Package errors provides custom error types and error handling utilities for the engine.
package errors

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/guileen/pglitedb/logger"
)

// Error codes for different types of errors
const (
	ErrCodeUnknown           = "unknown_error"
	ErrCodeTransaction       = "transaction_error"
	ErrCodeStorage           = "storage_error"
	ErrCodeCodec             = "codec_error"
	ErrCodeQuery             = "query_error"
	ErrCodeResource          = "resource_error"
	ErrCodeValidation        = "validation_error"
	ErrCodeNotFound          = "not_found"
	ErrCodeConflict          = "conflict"
	ErrCodeInvalidOperation  = "invalid_operation"
	ErrCodePermissionDenied  = "permission_denied"
)

// EngineError represents a custom error type for the engine
type EngineError struct {
	Code    string
	Message string
	Op      string
	Err     error
}

// Error implements the error interface
func (e *EngineError) Error() string {
	if e.Op != "" {
		return fmt.Sprintf("%s: %s", e.Op, e.Message)
	}
	return e.Message
}

// Unwrap implements the unwrap interface for error chaining
func (e *EngineError) Unwrap() error {
	return e.Err
}

// Is checks if the error matches the target error
func (e *EngineError) Is(target error) bool {
	if t, ok := target.(*EngineError); ok {
		return e.Code == t.Code
	}
	return false
}

// Log logs the error with the provided logger
func (e *EngineError) Log(ctx context.Context, logLevel slog.Level) {
	logFields := []any{
		"error_code", e.Code,
		"operation", e.Op,
		"message", e.Message,
	}
	
	// Append context values if available
	if ctx != nil {
		contextFields := logger.ExtractContextValues(ctx)
		logFields = append(logFields, contextFields...)
	}
	
	// Add the underlying error if it exists
	if e.Err != nil {
		logFields = append(logFields, "cause", e.Err.Error())
	}
	
	switch logLevel {
	case slog.LevelDebug:
		logger.DebugContext(ctx, "Engine error occurred", logFields...)
	case slog.LevelInfo:
		logger.InfoContext(ctx, "Engine error occurred", logFields...)
	case slog.LevelWarn:
		logger.WarnContext(ctx, "Engine error occurred", logFields...)
	case slog.LevelError:
		logger.Error("Engine error occurred", logFields...)
	default:
		logger.Error("Engine error occurred", logFields...)
	}
}

// New creates a new EngineError
func New(code, message string) *EngineError {
	return &EngineError{
		Code:    code,
		Message: message,
	}
}

// Errorf creates a new EngineError with formatted message
func Errorf(code, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
	}
}

// Wrap wraps an existing error with context
func Wrap(err error, code, op string) *EngineError {
	return &EngineError{
		Code:    code,
		Message: err.Error(),
		Op:      op,
		Err:     err,
	}
}

// Wrapf wraps an existing error with formatted context
func Wrapf(err error, code, op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    code,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
		Err:     err,
	}
}

// Common error constructors
func NewTransactionError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeTransaction,
		Message: msg,
		Op:      op,
	}
}

func NewTransactionErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeTransaction,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

func NewStorageError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeStorage,
		Message: msg,
		Op:      op,
	}
}

func NewStorageErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeStorage,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

func NewCodecError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeCodec,
		Message: msg,
		Op:      op,
	}
}

func NewCodecErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeCodec,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

func NewQueryError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeQuery,
		Message: msg,
		Op:      op,
	}
}

func NewQueryErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeQuery,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

func NewResourceError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeResource,
		Message: msg,
		Op:      op,
	}
}

func NewResourceErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeResource,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

func NewValidationError(op, msg string) *EngineError {
	return &EngineError{
		Code:    ErrCodeValidation,
		Message: msg,
		Op:      op,
	}
}

func NewValidationErrorf(op, format string, args ...interface{}) *EngineError {
	return &EngineError{
		Code:    ErrCodeValidation,
		Message: fmt.Sprintf(format, args...),
		Op:      op,
	}
}

// IsTransactionError checks if an error is a transaction error
func IsTransactionError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeTransaction
}

// IsStorageError checks if an error is a storage error
func IsStorageError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeStorage
}

// IsCodecError checks if an error is a codec error
func IsCodecError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeCodec
}

// IsQueryError checks if an error is a query error
func IsQueryError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeQuery
}

// IsResourceError checks if an error is a resource error
func IsResourceError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeResource
}

// IsValidationError checks if an error is a validation error
func IsValidationError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeValidation
}

// IsNotFound checks if an error indicates something was not found
func IsNotFound(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeNotFound
}

// IsConflict checks if an error indicates a conflict
func IsConflict(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeConflict
}

// Predefined error variables
var (
	ErrClosed      = &EngineError{Code: "closed", Message: "connection closed"}
	ErrConflict    = &EngineError{Code: ErrCodeConflict, Message: "conflict"}
	ErrRowNotFound = &EngineError{Code: ErrCodeNotFound, Message: "row not found"}
)

// IsClosedError checks if an error indicates a closed connection
func IsClosedError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == "closed"
}

// IsConflictError checks if an error indicates a conflict
func IsConflictError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeConflict
}

// IsRowNotFoundError checks if an error indicates a row not found
func IsRowNotFoundError(err error) bool {
	var e *EngineError
	return errors.As(err, &e) && e.Code == ErrCodeNotFound
}

// LogError logs an error at error level
func LogError(ctx context.Context, err error) {
	if e, ok := err.(*EngineError); ok {
		e.Log(ctx, slog.LevelError)
	} else {
		logger.Error("Unexpected error occurred", "error", err.Error())
	}
}

// LogWarning logs an error at warning level
func LogWarning(ctx context.Context, err error) {
	if e, ok := err.(*EngineError); ok {
		e.Log(ctx, slog.LevelWarn)
	} else {
		logger.WarnContext(ctx, "Unexpected error occurred", "error", err.Error())
	}
}

// LogInfo logs an error at info level
func LogInfo(ctx context.Context, err error) {
	if e, ok := err.(*EngineError); ok {
		e.Log(ctx, slog.LevelInfo)
	} else {
		logger.InfoContext(ctx, "Error occurred", "error", err.Error())
	}
}

// LogDebug logs an error at debug level
func LogDebug(ctx context.Context, err error) {
	if e, ok := err.(*EngineError); ok {
		e.Log(ctx, slog.LevelDebug)
	} else {
		logger.DebugContext(ctx, "Error occurred", "error", err.Error())
	}
}