package logger

import (
	"context"
	"log/slog"
	"sync"
)

// Logger is the global logger instance
var (
	Logger     *slog.Logger
	loggerOnce sync.Once
)

func init() {
	// Load configuration and create logger
	loggerOnce.Do(func() {
		config := LoadConfig()
		Logger = NewLogger(config)
	})
}

// Debug logs a debug message
func Debug(msg string, args ...any) {
	Logger.Debug(msg, args...)
}

// DebugContext logs a debug message with context
func DebugContext(ctx context.Context, msg string, args ...any) {
	if Logger.Enabled(ctx, slog.LevelDebug) {
		Logger.DebugContext(ctx, msg, append(appendContextArgs(ctx), args...)...)
	}
}

// Info logs an info message
func Info(msg string, args ...any) {
	Logger.Info(msg, args...)
}

// InfoContext logs an info message with context
func InfoContext(ctx context.Context, msg string, args ...any) {
	if Logger.Enabled(ctx, slog.LevelInfo) {
		Logger.InfoContext(ctx, msg, append(appendContextArgs(ctx), args...)...)
	}
}

// Warn logs a warning message
func Warn(msg string, args ...any) {
	Logger.Warn(msg, args...)
}

// WarnContext logs a warning message with context
func WarnContext(ctx context.Context, msg string, args ...any) {
	if Logger.Enabled(ctx, slog.LevelWarn) {
		Logger.WarnContext(ctx, msg, append(appendContextArgs(ctx), args...)...)
	}
}

// Error logs an error message
func Error(msg string, args ...any) {
	Logger.Error(msg, args...)
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