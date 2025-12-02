//go:build test

package scan

import (
	dbTypes "github.com/guileen/pglitedb/types"
)

// Mock iterator types for testing
type MockIndexIterator struct{}

func (ii *MockIndexIterator) Next() bool                           { return true }
func (ii *MockIndexIterator) Row() *dbTypes.Record                { return &dbTypes.Record{} }
func (ii *MockIndexIterator) Error() error                        { return nil }
func (ii *MockIndexIterator) Close() error                        { return nil }

type MockIndexOnlyIterator struct{}

func (ioi *MockIndexOnlyIterator) Next() bool                     { return true }
func (ioi *MockIndexOnlyIterator) Row() *dbTypes.Record          { return &dbTypes.Record{} }
func (ioi *MockIndexOnlyIterator) Error() error                  { return nil }
func (ioi *MockIndexOnlyIterator) Close() error                  { return nil }

type MockRowIterator struct{}

func (ri *MockRowIterator) Next() bool                            { return true }
func (ri *MockRowIterator) Row() *dbTypes.Record                 { return &dbTypes.Record{} }
func (ri *MockRowIterator) Error() error                         { return nil }
func (ri *MockRowIterator) Close() error                         { return nil }