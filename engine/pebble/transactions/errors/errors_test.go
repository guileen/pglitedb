package errors

import (
	"errors"
	"testing"
	
	dbTypes "github.com/guileen/pglitedb/types"
)

func TestErrorCompatibility(t *testing.T) {
	// Test that our ErrRowNotFound is compatible with dbTypes.ErrRecordNotFound
	if !errors.Is(ErrRowNotFound, dbTypes.ErrRecordNotFound) {
		t.Errorf("ErrRowNotFound should be compatible with dbTypes.ErrRecordNotFound")
	}
	
	// Test that wrapped errors work correctly
	wrapped := Wrap(ErrRowNotFound, "test_code", "test message")
	if !errors.Is(wrapped, ErrRowNotFound) {
		t.Errorf("Wrapped error should be ErrRowNotFound")
	}
	
	// Test that wrapped errors are still compatible with dbTypes.ErrRecordNotFound
	if !errors.Is(wrapped, dbTypes.ErrRecordNotFound) {
		t.Errorf("Wrapped error should be compatible with dbTypes.ErrRecordNotFound")
	}
}

func TestErrorCodes(t *testing.T) {
	// Test error codes
	if ErrClosed.Code() != "closed" {
		t.Errorf("Expected code 'closed', got '%s'", ErrClosed.Code())
	}
	
	if ErrRowNotFound.Code() != "row_not_found" {
		t.Errorf("Expected code 'row_not_found', got '%s'", ErrRowNotFound.Code())
	}
}