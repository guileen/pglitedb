# Logging System Context

## Overview
The PGLiteDB project now implements a comprehensive logging system based on Go's slog package (available since Go 1.21). This provides structured, context-aware logging throughout the application.

## Key Components

### Logger Package
Located at `logger/logger.go` and `logger/config.go`, this package provides:

1. **Structured Logging**: JSON and text format support
2. **Log Levels**: DEBUG, INFO, WARN, ERROR
3. **Environment Configuration**: LOG_LEVEL and LOG_FORMAT environment variables
4. **Context Enrichment**: Automatic inclusion of timestamps, source locations, and contextual information

### Key Functions
- `logger.Debug()` - Debug level messages
- `logger.Info()` - Informational messages
- `logger.Warn()` - Warning messages
- `logger.Error()` - Error messages

## Implementation Details

### Configuration
The logger can be configured through environment variables:
- `LOG_LEVEL` - Set log level (DEBUG, INFO, WARN, ERROR)
- `LOG_FORMAT` - Set output format (json, text)

### Usage Patterns
```go
// Basic logging
logger.Info("Database connection established", "host", host, "port", port)

// Error logging with context
logger.Error("Failed to execute query", "error", err, "query", query)

// Debug logging for detailed tracing
logger.Debug("Processing request", "request_id", reqID, "user_id", userID)
```

## Integration Points

### Server Startup
Comprehensive logging during server initialization in `cmd/server/main.go`:
- Startup time tracking with timestamps
- Component initialization logging
- Duration measurements for critical operations
- Graceful shutdown logging

### PostgreSQL Server
Detailed logging in `protocol/pgserver/server.go`:
- Connection handling with remote/local addresses
- Message processing with type and count information
- Query parsing and execution timing
- Extended query protocol operations (Parse, Bind, Describe, Execute)

### HTTP Server
Structured logging in HTTP server components:
- Request/response logging
- Error handling with context
- Performance monitoring

## Best Practices

### Context Enrichment
Always include relevant context information:
```go
logger.Info("User login", 
    "user_id", userID,
    "ip_address", ipAddress,
    "timestamp", time.Now().Unix())
```

### Log Level Usage
- **DEBUG**: Detailed diagnostic information, typically only enabled in development
- **INFO**: General operational messages about successful operations
- **WARN**: Potentially harmful situations that don't prevent functionality
- **ERROR**: Error events that might still allow the application to continue running

### Performance Considerations
- Avoid expensive string concatenation in log messages
- Use structured key-value pairs for better parsing and filtering
- Be mindful of log volume in production environments

## Recent Enhancements

### Service Startup Monitoring
Enhanced logging for service startup including:
- Timestamped initialization events
- Duration measurements for component setup
- Clear success/failure indicators
- Graceful shutdown tracking

### Background Service Logging
Improved observability for background services:
- Connection counting and tracking
- Periodic progress reporting for long-running operations
- Detailed error context for troubleshooting

### Error Handling
Standardized error logging with contextual information:
- Error messages with stack traces
- Operation context for easier debugging
- Consistent formatting across all components

## Future Improvements

### Large File Refactoring Support
As part of the maintainability initiative, the logging system will support:
- Detailed tracing during file processing
- Performance metrics for refactored components
- Progress reporting for long-running refactor operations

### Enhanced Context Awareness
Future enhancements will include:
- Tenant and user context propagation
- Request tracing across components
- Enhanced performance monitoring capabilities

## Related Files
- `logger/logger.go` - Core logger implementation
- `logger/config.go` - Logger configuration
- `cmd/server/main.go` - Server startup logging
- `protocol/pgserver/server.go` - PostgreSQL server logging

## Access Requirements

❗ All context users must provide:
1. Reflections on their task outcomes
2. Ratings of context usefulness (1-10 scale)
3. Specific feedback on referenced sections

This feedback is essential for continuous context improvement and must be submitted with every context access.

See [REFLECT.md](./REFLECT.md) for detailed reflection guidelines and examples.