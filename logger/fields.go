package logger

import (
	"log/slog"
	"time"
)

// Field helpers for structured logging
var (
	// Common field constructors
	String  = slog.String
	Int     = slog.Int
	Int64   = slog.Int64
	Float64 = slog.Float64
	Bool    = slog.Bool
	Time    = slog.Time
	Any     = slog.Any
	
	// Specialized field constructors
	Duration = func(key string, d time.Duration) slog.Attr {
		return slog.Any(key, d)
	}
	
	ErrorField = func(err error) slog.Attr {
		if err == nil {
			return slog.String("error", "<nil>")
		}
		return slog.String("error", err.Error())
	}
	
	// Component-specific fields
	Component = func(name string) slog.Attr {
		return slog.String("component", name)
	}
	
	Operation = func(name string) slog.Attr {
		return slog.String("operation", name)
	}
	
	Version = func(version string) slog.Attr {
		return slog.String("version", version)
	}
)