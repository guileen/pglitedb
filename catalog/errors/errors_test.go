package errors

import (
	"errors"
	"fmt"
	"testing"
	
	"github.com/stretchr/testify/assert"
)

func TestCatalogError_Error(t *testing.T) {
	// Test basic error message
	err := &CatalogError{code: "test_code", msg: "test message"}
	assert.Equal(t, "test message", err.Error())
	
	// Test error with wrapped error
	wrappedErr := fmt.Errorf("wrapped error")
	errWithWrapped := &CatalogError{code: "test_code", msg: "test message", err: wrappedErr}
	expected := "test message: wrapped error"
	assert.Equal(t, expected, errWithWrapped.Error())
}

func TestCatalogError_Code(t *testing.T) {
	err := &CatalogError{code: "test_code", msg: "test message"}
	assert.Equal(t, "test_code", err.Code())
}

func TestCatalogError_Unwrap(t *testing.T) {
	wrappedErr := fmt.Errorf("wrapped error")
	err := &CatalogError{code: "test_code", msg: "test message", err: wrappedErr}
	assert.Equal(t, wrappedErr, err.Unwrap())
	
	// Test unwrap with nil error
	errNil := &CatalogError{code: "test_code", msg: "test message"}
	assert.Nil(t, errNil.Unwrap())
}

func TestCatalogError_Is(t *testing.T) {
	err1 := &CatalogError{code: "test_code", msg: "test message"}
	err2 := &CatalogError{code: "test_code", msg: "different message"}
	err3 := &CatalogError{code: "different_code", msg: "test message"}
	
	// Same code should match
	assert.True(t, err1.Is(err2))
	
	// Different codes should not match
	assert.False(t, err1.Is(err3))
	
	// Nil comparison
	assert.False(t, err1.Is(nil))
}

func TestNew(t *testing.T) {
	err := New("test_code", "formatted %s %d", "message", 42)
	assert.Equal(t, "test_code", err.Code())
	assert.Equal(t, "formatted message 42", err.Error())
}

func TestWrap(t *testing.T) {
	wrappedErr := fmt.Errorf("wrapped error")
	err := Wrap(wrappedErr, "test_code", "formatted %s", "message")
	
	assert.Equal(t, "test_code", err.Code())
	assert.Equal(t, "formatted message: wrapped error", err.Error())
	assert.Equal(t, wrappedErr, err.Unwrap())
}

func TestIsCatalogError(t *testing.T) {
	err := &CatalogError{code: "test_code", msg: "test message"}
	assert.True(t, IsCatalogError(err))
	
	// Test with non-CatalogError
	stdErr := fmt.Errorf("standard error")
	assert.False(t, IsCatalogError(stdErr))
}

func TestPredefinedErrors(t *testing.T) {
	// Test that all predefined errors are properly initialized
	assert.NotNil(t, ErrTableNotFound)
	assert.NotNil(t, ErrTableAlreadyExists)
	assert.NotNil(t, ErrViewNotFound)
	assert.NotNil(t, ErrViewAlreadyExists)
	assert.NotNil(t, ErrInvalidColumnType)
	assert.NotNil(t, ErrColumnNotFound)
	assert.NotNil(t, ErrConstraintViolation)
	assert.NotNil(t, ErrInvalidConstraint)
	assert.NotNil(t, ErrForeignKeyViolation)
	assert.NotNil(t, ErrUniqueConstraintViolation)
	assert.NotNil(t, ErrPrimaryKeyConstraintViolation)
	assert.NotNil(t, ErrCheckConstraintViolation)
	
	// Test that they have proper codes
	assert.Equal(t, "table_not_found", ErrTableNotFound.Code())
	assert.Equal(t, "table_already_exists", ErrTableAlreadyExists.Code())
	assert.Equal(t, "view_not_found", ErrViewNotFound.Code())
	assert.Equal(t, "view_already_exists", ErrViewAlreadyExists.Code())
}

func TestIsTableNotFoundError(t *testing.T) {
	assert.True(t, IsTableNotFoundError(ErrTableNotFound))
	
	// Test with different error code
	assert.False(t, IsTableNotFoundError(ErrTableAlreadyExists))
	
	// Test with wrapped error
	wrapped := &CatalogError{code: "table_not_found", msg: "wrapped"}
	assert.True(t, IsTableNotFoundError(wrapped))
}

func TestIsTableAlreadyExistsError(t *testing.T) {
	assert.True(t, IsTableAlreadyExistsError(ErrTableAlreadyExists))
	assert.False(t, IsTableAlreadyExistsError(ErrTableNotFound))
}

func TestIsViewNotFoundError(t *testing.T) {
	assert.True(t, IsViewNotFoundError(ErrViewNotFound))
	assert.False(t, IsViewNotFoundError(ErrViewAlreadyExists))
}

func TestIsViewAlreadyExistsError(t *testing.T) {
	assert.True(t, IsViewAlreadyExistsError(ErrViewAlreadyExists))
	assert.False(t, IsViewAlreadyExistsError(ErrViewNotFound))
}

func TestIsInvalidColumnTypeError(t *testing.T) {
	assert.True(t, IsInvalidColumnTypeError(ErrInvalidColumnType))
	assert.False(t, IsInvalidColumnTypeError(ErrColumnNotFound))
}

func TestCompatibleError(t *testing.T) {
	compatibleErr := errors.New("compatible error")
	compatible := &compatibleError{
		CatalogError: &CatalogError{code: "test_code", msg: "test message"},
		compatible:   compatibleErr,
	}
	
	assert.Equal(t, "test message", compatible.Error())
	assert.Equal(t, compatible.CatalogError, compatible.Unwrap())
	
	// Test Is method with compatible error
	assert.True(t, compatible.Is(compatibleErr))
	
	// Test Is method with CatalogError
	sameCodeErr := &CatalogError{code: "test_code", msg: "different message"}
	assert.True(t, compatible.Is(sameCodeErr))
}

func TestIsFunction(t *testing.T) {
	// Test nil error
	assert.True(t, Is(nil, nil))
	assert.False(t, Is(nil, errors.New("some error")))
	
	// Test direct equality
	err := errors.New("test error")
	assert.True(t, Is(err, err))
	
	// Test with CatalogError
	catErr := &CatalogError{code: "test_code", msg: "test message"}
	sameCodeErr := &CatalogError{code: "test_code", msg: "different message"}
	diffCodeErr := &CatalogError{code: "diff_code", msg: "test message"}
	
	assert.True(t, Is(catErr, sameCodeErr))
	assert.False(t, Is(catErr, diffCodeErr))
	
	// Test with wrapped error
	wrapped := fmt.Errorf("wrapped: %w", catErr)
	assert.True(t, Is(wrapped, catErr))
}