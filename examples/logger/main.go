package main

import (
	"github.com/guileen/pglitedb/logger"
)

func main() {
	// Basic usage examples
	logger.Debug("This is a debug message", "component", "main")
	logger.Info("Application started", "version", "1.0.0")
	logger.Warn("This is a warning message", "retry_count", 3)
	logger.Error("This is an error message", "error_code", 500)

	// Using With to add context
	dbLogger := logger.With("component", "database")
	dbLogger.Info("Connected to database", "host", "localhost", "port", 5432)

	// You can also set log level programmatically
	// logger.SetLogLevel(slog.LevelDebug)
}