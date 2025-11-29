package errors

import (
	"errors"
	"testing"
)

func TestEngineError_Error(t *testing.T) {
	err := &EngineError{code: "test_code", msg: "test message"}
	expected := "[test_code] test message"
	if err.Error() != expected {
		t.Errorf("Expected %s, got %s", expected, err.Error())
	}
}

func TestEngineError_Wrap(t *testing.T) {
	innerErr := errors.New("inner error")
	err := Wrap(innerErr, "test_code", "test message")
	expected := "[test_code] test message: inner error"
	if err.Error() != expected {
		t.Errorf("Expected %s, got %s", expected, err.Error())
	}
}

func TestIsClosedError(t *testing.T) {
	if !IsClosedError(ErrClosed) {
		t.Error("Expected ErrClosed to be identified as closed error")
	}
	
	wrappedErr := Wrap(ErrClosed, "wrapper", "wrapped")
	if IsClosedError(wrappedErr) {
		t.Error("Expected wrapped error not to be identified as closed error")
	}
}

func TestIsConflictError(t *testing.T) {
	if !IsConflictError(ErrConflict) {
		t.Error("Expected ErrConflict to be identified as conflict error")
	}
}

func TestIsRowNotFoundError(t *testing.T) {
	if !IsRowNotFoundError(ErrRowNotFound) {
		t.Error("Expected ErrRowNotFound to be identified as row not found error")
	}
}