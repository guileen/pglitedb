package logger

import (
	"io"
	"log/slog"
	"os"
	"sync"
)

// LoggerFactory manages logger creation and configuration
type LoggerFactory struct {
	config Config
	writer io.Writer
	mutex  sync.RWMutex
}

// NewLoggerFactory creates a new LoggerFactory with the given configuration
func NewLoggerFactory(config Config) *LoggerFactory {
	writer := config.Writer
	if writer == nil {
		writer = os.Stdout
	}
	
	return &LoggerFactory{
		config: config,
		writer: writer,
	}
}

// CreateLogger creates a new logger with the factory's configuration
func (f *LoggerFactory) CreateLogger() *slog.Logger {
	f.mutex.RLock()
	defer f.mutex.RUnlock()
	
	opts := &slog.HandlerOptions{
		Level:     f.config.Level,
		AddSource: f.config.AddSource,
	}
	
	var handler slog.Handler
	
	switch f.config.Format {
	case "text":
		handler = slog.NewTextHandler(f.writer, opts)
	default: // json
		handler = slog.NewJSONHandler(f.writer, opts)
	}
	
	return slog.New(handler)
}

// UpdateConfig updates the factory's configuration
func (f *LoggerFactory) UpdateConfig(config Config) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	
	f.config = config
	
	// Update writer if provided
	if config.Writer != nil {
		f.writer = config.Writer
	}
}