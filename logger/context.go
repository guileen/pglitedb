package logger

import (
	"context"
)

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

// WithContextValue adds a value to the context for logging
func WithContextValue(ctx context.Context, key ContextKey, value any) context.Context {
	return context.WithValue(ctx, key, value)
}

// ExtractContextValues extracts logging-relevant values from context
func ExtractContextValues(ctx context.Context) []any {
	if ctx == nil {
		return nil
	}

	var args []any

	// Extract common context values
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