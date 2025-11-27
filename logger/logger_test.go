package logger

import (
	"context"
	"testing"
)

func TestContextLogging(t *testing.T) {
	// Create a context with tenant and user information
	ctx := context.Background()
	ctx = context.WithValue(ctx, TenantIDKey, int64(123))
	ctx = context.WithValue(ctx, UserIDKey, "user456")
	ctx = context.WithValue(ctx, RequestIDKey, "req789")

	// Test context-aware logging
	InfoContext(ctx, "Test message with context")

	// Test appending to existing args
	InfoContext(ctx, "Test message with context and args", "key", "value")
}