package logger

import (
	"io"
	"log/slog"
	"os"
	"strconv"
)

// Config holds the logger configuration
type Config struct {
	Level      slog.Level
	Format     string // "json" or "text"
	AddSource  bool   // Whether to add source code information
	AddContext bool   // Whether to add context information
	Writer     io.Writer // Custom writer for output
}

// DefaultConfig returns the default logger configuration
func DefaultConfig() Config {
	return Config{
		Level:      slog.LevelInfo,
		Format:     "json",
		AddSource:  true,
		AddContext: true,
		Writer:     os.Stdout,
	}
}

// LoadConfig loads the logger configuration from environment variables
func LoadConfig() Config {
	config := DefaultConfig()

	// Load log level from LOG_LEVEL environment variable
	if levelStr := os.Getenv("LOG_LEVEL"); levelStr != "" {
		switch levelStr {
		case "DEBUG":
			config.Level = slog.LevelDebug
		case "INFO":
			config.Level = slog.LevelInfo
		case "WARN":
			config.Level = slog.LevelWarn
		case "ERROR":
			config.Level = slog.LevelError
		default:
			// Try to parse as integer level
			if levelInt, err := strconv.Atoi(levelStr); err == nil {
				config.Level = slog.Level(levelInt)
			}
		}
	}

	// Load format from LOG_FORMAT environment variable
	if format := os.Getenv("LOG_FORMAT"); format != "" {
		if format == "text" || format == "json" {
			config.Format = format
		}
	}

	// Load source inclusion from LOG_ADD_SOURCE environment variable
	if addSourceStr := os.Getenv("LOG_ADD_SOURCE"); addSourceStr != "" {
		if addSource, err := strconv.ParseBool(addSourceStr); err == nil {
			config.AddSource = addSource
		}
	}

	// Load context inclusion from LOG_ADD_CONTEXT environment variable
	if addContextStr := os.Getenv("LOG_ADD_CONTEXT"); addContextStr != "" {
		if addContext, err := strconv.ParseBool(addContextStr); err == nil {
			config.AddContext = addContext
		}
	}

	return config
}

// NewLogger creates a new logger with the given configuration
func NewLogger(config Config) *slog.Logger {
	opts := &slog.HandlerOptions{
		Level:     config.Level,
		AddSource: config.AddSource,
	}

	var handler slog.Handler

	switch config.Format {
	case "text":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default: // json
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler)
}