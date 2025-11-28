package main

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/logger"
)

func main() {
	// Basic logging
	logger.Info("Application started", "version", "1.0.0")
	
	// Context-aware logging
	ctx := context.Background()
	ctx = logger.WithContextValue(ctx, logger.RequestIDKey, "req-12345")
	ctx = logger.WithContextValue(ctx, logger.UserIDKey, "user-67890")
	
	logger.InfoContext(ctx, "Processing request", "method", "GET", "path", "/api/users")
	
	// Structured logging with fields
	logger.Warn("High memory usage detected", 
		logger.Float64("memory_usage_percent", 85.5),
		logger.String("component", "memory_monitor"),
		logger.Duration("duration", time.Since(time.Now().Add(-time.Minute))))
	
	// Error logging
	err := fmt.Errorf("database connection failed")
	logger.Error("Failed to connect to database", 
		logger.ErrorField(err),
		logger.String("host", "localhost"),
		logger.Int("port", 5432))
	
	// Debug logging (only shown if log level is DEBUG)
	logger.Debug("Debug information", 
		logger.String("debug_key", "debug_value"),
		logger.Any("complex_data", map[string]interface{}{
			"nested": map[string]interface{}{
				"value": 42,
			},
		}))
	
	// Using With for adding context
	userLogger := logger.With(
		logger.String("component", "user_service"),
		logger.String("service_version", "2.1.0"))
	
	userLogger.Info("User authenticated", 
		logger.String("user_id", "user-67890"),
		logger.String("auth_method", "oauth2"))
	
	// Using WithGroup for grouping related fields
	dbLogger := logger.With(logger.Component("database"))
	dbLogger.Info("Query executed",
		logger.String("query", "SELECT * FROM users WHERE id = ?"),
		logger.Duration("duration", 5*time.Millisecond),
		logger.Int64("rows_affected", 1))
}