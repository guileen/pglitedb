package utils

import (
	"errors"
	"testing"
)

func TestErrorHandler(t *testing.T) {
	eh := NewErrorHandler()
	
	// Test WrapError
	err := errors.New("original error")
	wrapped := eh.WrapError(err, "additional context: %s", "test")
	if wrapped == nil {
		t.Fatal("WrapError returned nil")
	}
	
	if !errors.Is(wrapped, err) {
		t.Error("Wrapped error should contain original error")
	}
	
	// Test standard error types
	if eh.IsNotFoundError(ErrNotFound) != true {
		t.Error("IsNotFoundError should return true for ErrNotFound")
	}
	
	if eh.IsConflictError(ErrConflict) != true {
		t.Error("IsConflictError should return true for ErrConflict")
	}
	
	if eh.IsValidationError(ErrValidation) != true {
		t.Error("IsValidationError should return true for ErrValidation")
	}
	
	if eh.IsTimeoutError(ErrTimeout) != true {
		t.Error("IsTimeoutError should return true for ErrTimeout")
	}
	
	// Test EngineError creation
	engineErr := eh.NewEngineError("TEST", "001", "test error", ErrValidation)
	if engineErr == nil {
		t.Fatal("NewEngineError returned nil")
	}
	
	if engineErr.Code != "001" {
		t.Errorf("Expected code '001', got '%s'", engineErr.Code)
	}
	
	if engineErr.Category != "TEST" {
		t.Errorf("Expected category 'TEST', got '%s'", engineErr.Category)
	}
	
	if engineErr.Message != "test error" {
		t.Errorf("Expected message 'test error', got '%s'", engineErr.Message)
	}
	
	// Test error type checking with EngineError
	if eh.IsValidationError(engineErr) != true {
		t.Error("IsValidationError should return true for EngineError with VALIDATION category")
	}
}