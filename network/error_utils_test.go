package network

import (
	"context"
	"errors"
	"testing"
)

func TestNetworkError(t *testing.T) {
	// Test NetworkError creation
	origErr := errors.New("connection refused")
	netErr := NewNetworkError("connect", "localhost:5432", origErr)
	
	if netErr == nil {
		t.Fatal("NewNetworkError returned nil")
	}
	
	if netErr.Operation != "connect" {
		t.Errorf("Expected operation 'connect', got '%s'", netErr.Operation)
	}
	
	if netErr.Address != "localhost:5432" {
		t.Errorf("Expected address 'localhost:5432', got '%s'", netErr.Address)
	}
	
	if !errors.Is(netErr, origErr) {
		t.Error("NetworkError should wrap the original error")
	}
	
	// Test IsNetworkError
	if !IsNetworkError(netErr) {
		t.Error("IsNetworkError should return true for NetworkError")
	}
	
	if IsNetworkError(origErr) {
		t.Error("IsNetworkError should return false for non-NetworkError")
	}
}

func TestErrorTypeChecking(t *testing.T) {
	// Test IsTimeoutError
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	
	if !IsTimeoutError(ctx.Err()) {
		t.Error("IsTimeoutError should return true for cancelled context")
	}
	
	// Test IsConnectionError
	connRefused := errors.New("connection refused")
	if !IsConnectionError(connRefused) {
		t.Error("IsConnectionError should return true for 'connection refused'")
	}
	
	brokenPipe := errors.New("broken pipe")
	if !IsConnectionError(brokenPipe) {
		t.Error("IsConnectionError should return true for 'broken pipe'")
	}
	
	// Test IsNotFoundError
	notFound := errors.New("record not found")
	if !IsNotFoundError(notFound) {
		t.Error("IsNotFoundError should return true for 'record not found'")
	}
	
	// Test IsConflictError
	conflict := errors.New("duplicate key value violates unique constraint")
	if !IsConflictError(conflict) {
		t.Error("IsConflictError should return true for duplicate key error")
	}
}

func TestWrapError(t *testing.T) {
	origErr := errors.New("original error")
	wrapped := WrapError(origErr, "additional context: %s", "test")
	
	if wrapped == nil {
		t.Fatal("WrapError returned nil")
	}
	
	if !errors.Is(wrapped, origErr) {
		t.Error("Wrapped error should contain original error")
	}
	
	expectedMsg := "additional context: test: original error"
	if wrapped.Error() != expectedMsg {
		t.Errorf("Expected message '%s', got '%s'", expectedMsg, wrapped.Error())
	}
}