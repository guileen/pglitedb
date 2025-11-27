package logger

import (
	"context"
	"log/slog"
)

// Logger is the global logger instance
var Logger *slog.Logger

// ContextKey is used for context values
type ContextKey string

const (
	// TenantIDKey is the context key for tenant ID
	TenantIDKey ContextKey = "tenant_id"
	// UserIDKey is the context key for user ID
	UserIDKey ContextKey = "user_id"
	// RequestIDKey is the context key for request ID
	RequestIDKey ContextKey = "request_id"
)

func init() {
	// Load configuration and create logger
	config := LoadConfig()
	Logger = NewLogger(config)
}

// Debug logs a debug message
func Debug(msg string, args ...any) {
	Logger.Debug(msg, args...)
}

// DebugContext logs a debug message with context
func DebugContext(ctx context.Context, msg string, args ...any) {
	Logger.Debug(msg, appendContextArgs(ctx, args...)...)
}

// Info logs an info message
func Info(msg string, args ...any) {
	Logger.Info(msg, args...)
}

// InfoContext logs an info message with context
func InfoContext(ctx context.Context, msg string, args ...any) {
	Logger.Info(msg, appendContextArgs(ctx, args...)...)
}

// Warn logs a warning message
func Warn(msg string, args ...any) {
	Logger.Warn(msg, args...)
}

// WarnContext logs a warning message with context
func WarnContext(ctx context.Context, msg string, args ...any) {
	Logger.Warn(msg, appendContextArgs(ctx, args...)...)
}

// Error logs an error message
func Error(msg string, args ...any) {
	Logger.Error(msg, args...)
}

// ErrorContext logs an error message with context
func ErrorContext(ctx context.Context, msg string, args ...any) {
	Logger.Error(msg, appendContextArgs(ctx, args...)...)
}

// With returns a new Logger that includes the given attributes in each output operation
func With(args ...any) *slog.Logger {
	return Logger.With(args...)
}

// WithContext returns a new Logger that includes context information
func WithContext(ctx context.Context) *slog.Logger {
	return Logger.With(appendContextArgs(ctx)...)
}

// SetLogLevel programmatically sets the log level
func SetLogLevel(level slog.Level) {
	// Note: This is a simplified approach. In a production environment,
	// you might want to use a more sophisticated method to change log levels dynamically
	config := LoadConfig()
	config.Level = level
	Logger = NewLogger(config)
}

// appendContextArgs extracts context values and appends them to the args
func appendContextArgs(ctx context.Context, args ...any) []any {
	if ctx == nil {
		return args
	}

	// Extract context values
	if tenantID, ok := ctx.Value(TenantIDKey).(int64); ok {
		args = append(args, "tenant_id", tenantID)
	}

	if userID, ok := ctx.Value(UserIDKey).(string); ok {
		args = append(args, "user_id", userID)
	}

	if requestID, ok := ctx.Value(RequestIDKey).(string); ok {
		args = append(args, "request_id", requestID)
	}

	return args
}