package scan

import (
	dbTypes "github.com/guileen/pglitedb/types"
)

// IndexIteratorInterface defines the interface for IndexIterator
type IndexIteratorInterface interface {
	Next() bool
	Row() *dbTypes.Record
	Error() error
	Close() error
}

// IndexOnlyIteratorInterface defines the interface for IndexOnlyIterator
type IndexOnlyIteratorInterface interface {
	Next() bool
	Row() *dbTypes.Record
	Error() error
	Close() error
}

// RowIteratorInterface defines the interface for RowIterator
type RowIteratorInterface interface {
	Next() bool
	Row() *dbTypes.Record
	Error() error
	Close() error
}

// PooledIterator is a generic interface for all pooled iterators
type PooledIterator interface {
	Next() bool
	Row() *dbTypes.Record
	Error() error
	Close() error
	Reset()
}