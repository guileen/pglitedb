// Package errors provides standardized error types for catalog operations.
package errors

import (
	"fmt"
)

// Error constants for catalog operations
var (
	// ErrTableNotFound is returned when a requested table cannot be found
	ErrTableNotFound = &CatalogError{code: "table_not_found", msg: "table not found"}

	// ErrTableAlreadyExists is returned when trying to create a table that already exists
	ErrTableAlreadyExists = &CatalogError{code: "table_already_exists", msg: "table already exists"}

	// ErrViewNotFound is returned when a requested view cannot be found
	ErrViewNotFound = &CatalogError{code: "view_not_found", msg: "view not found"}

	// ErrViewAlreadyExists is returned when trying to create a view that already exists
	ErrViewAlreadyExists = &CatalogError{code: "view_already_exists", msg: "view already exists"}

	// ErrInvalidColumnType is returned when a column has an invalid type
	ErrInvalidColumnType = &CatalogError{code: "invalid_column_type", msg: "invalid column type"}

	// ErrColumnNotFound is returned when a requested column cannot be found
	ErrColumnNotFound = &CatalogError{code: "column_not_found", msg: "column not found"}

	// ErrConstraintViolation is returned when a constraint validation fails
	ErrConstraintViolation = &CatalogError{code: "constraint_violation", msg: "constraint violation"}

	// ErrInvalidConstraint is returned when a constraint is invalid
	ErrInvalidConstraint = &CatalogError{code: "invalid_constraint", msg: "invalid constraint"}

	// ErrForeignKeyViolation is returned when a foreign key constraint is violated
	ErrForeignKeyViolation = &CatalogError{code: "foreign_key_violation", msg: "foreign key constraint violation"}

	// ErrUniqueConstraintViolation is returned when a unique constraint is violated
	ErrUniqueConstraintViolation = &CatalogError{code: "unique_constraint_violation", msg: "unique constraint violation"}

	// ErrPrimaryKeyConstraintViolation is returned when a primary key constraint is violated
	ErrPrimaryKeyConstraintViolation = &CatalogError{code: "primary_key_constraint_violation", msg: "primary key constraint violation"}

	// ErrCheckConstraintViolation is returned when a check constraint is violated
	ErrCheckConstraintViolation = &CatalogError{code: "check_constraint_violation", msg: "check constraint violation"}
)

// compatibleError wraps CatalogError and maintains compatibility with legacy errors
type compatibleError struct {
	*CatalogError
	compatible error
}

// Error implements the error interface
func (e *compatibleError) Error() string {
	return e.CatalogError.Error()
}

// Unwrap returns the wrapped error
func (e *compatibleError) Unwrap() error {
	return e.CatalogError
}

// Is checks if the error matches the target
func (e *compatibleError) Is(target error) bool {
	// Check if target is the compatible error
	if target == e.compatible {
		return true
	}
	// Delegate to CatalogError.Is
	return e.CatalogError.Is(target)
}

// CatalogError represents a catalog-specific error
type CatalogError struct {
	code string
	msg  string
	err  error // wrapped error
}

// Error implements the error interface
func (e *CatalogError) Error() string {
	if e.err != nil {
		return fmt.Sprintf("%s: %v", e.msg, e.err)
	}
	return e.msg
}

// Code returns the error code
func (e *CatalogError) Code() string {
	return e.code
}

// Unwrap returns the wrapped error
func (e *CatalogError) Unwrap() error {
	return e.err
}

// Is checks if the error matches the target
func (e *CatalogError) Is(target error) bool {
	if t, ok := target.(*CatalogError); ok {
		return e.code == t.code
	}
	return false
}

// New creates a new CatalogError with a formatted message
func New(code, format string, args ...interface{}) *CatalogError {
	return &CatalogError{
		code: code,
		msg:  fmt.Sprintf(format, args...),
	}
}

// Wrap wraps an existing error with a catalog error
func Wrap(err error, code, format string, args ...interface{}) *CatalogError {
	return &CatalogError{
		code: code,
		msg:  fmt.Sprintf(format, args...),
		err:  err,
	}
}

// IsCatalogError checks if an error is a CatalogError
func IsCatalogError(err error) bool {
	_, ok := err.(*CatalogError)
	return ok
}

// IsTableNotFoundError checks if an error indicates a table not found
func IsTableNotFoundError(err error) bool {
	return Is(err, ErrTableNotFound)
}

// IsTableAlreadyExistsError checks if an error indicates a table already exists
func IsTableAlreadyExistsError(err error) bool {
	return Is(err, ErrTableAlreadyExists)
}

// IsViewNotFoundError checks if an error indicates a view not found
func IsViewNotFoundError(err error) bool {
	return Is(err, ErrViewNotFound)
}

// IsViewAlreadyExistsError checks if an error indicates a view already exists
func IsViewAlreadyExistsError(err error) bool {
	return Is(err, ErrViewAlreadyExists)
}

// IsInvalidColumnTypeError checks if an error indicates an invalid column type
func IsInvalidColumnTypeError(err error) bool {
	return Is(err, ErrInvalidColumnType)
}

// Is implements the errors.Is functionality for CatalogError
func Is(err, target error) bool {
	if err == nil {
		return err == target
	}

	// Check direct equality first
	if err == target {
		return true
	}

	// Check if err implements Is method
	if e, ok := err.(interface{ Is(error) bool }); ok {
		return e.Is(target)
	}

	// Check unwrapped errors
	if e, ok := err.(interface{ Unwrap() error }); ok {
		return Is(e.Unwrap(), target)
	}

	return false
}