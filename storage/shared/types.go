// Package shared provides shared types and interfaces for the storage module
package shared

import (
	"context"
	"io"
)

// WriteOptions defines options for write operations
type WriteOptions struct {
	Sync       bool
	Durability DurabilityLevel
}

// DurabilityLevel defines the durability guarantee level
type DurabilityLevel int

const (
	DurabilityEventual    DurabilityLevel = iota
	DurabilityGroupCommit
	DurabilityImmediate
)

// IsolationLevel defines the isolation level for transactions
type IsolationLevel int

const (
	// ReadUncommitted allows reading uncommitted changes from other transactions
	ReadUncommitted IsolationLevel = iota
	// ReadCommitted ensures that only committed data can be read
	ReadCommitted
	// RepeatableRead ensures that data read in a transaction remains consistent
	RepeatableRead
	// SnapshotIsolation provides a consistent snapshot of the database at transaction start
	SnapshotIsolation
	// Serializable provides the highest isolation level, preventing all anomalies
	Serializable
)

var (
	DefaultWriteOptions  = &WriteOptions{Sync: false, Durability: DurabilityEventual}
	SyncWriteOptions     = &WriteOptions{Sync: true, Durability: DurabilityImmediate}
	EventualWriteOptions = &WriteOptions{Sync: false, Durability: DurabilityEventual}
)

// KV defines the interface for key-value storage operations
type KV interface {
	Get(ctx context.Context, key []byte) ([]byte, error)
	Set(ctx context.Context, key, value []byte) error
	SetWithOptions(ctx context.Context, key, value []byte, opts *WriteOptions) error
	Delete(ctx context.Context, key []byte) error
	DeleteWithOptions(ctx context.Context, key []byte, opts *WriteOptions) error
	NewBatch() Batch
	Commit(ctx context.Context, batch Batch) error
	CommitBatch(ctx context.Context, batch Batch) error
	CommitBatchWithOptions(ctx context.Context, batch Batch, opts *WriteOptions) error
	NewIterator(opts *IteratorOptions) Iterator
	NewSnapshot() (Snapshot, error)
	NewTransaction(ctx context.Context) (Transaction, error)
	Stats() KVStats
	Flush() error
	Close() error
	
	CheckForConflicts(txn Transaction, key []byte) error
}

// Batch defines the interface for batch operations
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	Count() int
	Reset()
	Close() error
}

// IteratorOptions defines options for iterator operations
type IteratorOptions struct {
	LowerBound []byte
	UpperBound []byte
	Prefix     []byte
	Reverse    bool
}

// Iterator defines the interface for iterating over key-value pairs
type Iterator interface {
	io.Closer
	Valid() bool
	Next() bool
	Prev() bool
	Key() []byte
	Value() []byte
	Error() error
	SeekGE(key []byte) bool
	SeekLT(key []byte) bool
	First() bool
	Last() bool
}

// Snapshot defines the interface for snapshot operations
type Snapshot interface {
	io.Closer
	Get(key []byte) ([]byte, error)
	NewIterator(opts *IteratorOptions) Iterator
}

// Transaction defines the interface for transaction operations
type Transaction interface {
	io.Closer
	Get(key []byte) ([]byte, error)
	Set(key, value []byte) error
	Delete(key []byte) error
	NewIterator(opts *IteratorOptions) Iterator
	Commit() error
	Rollback() error
	
	// Isolation returns the isolation level of the transaction
	Isolation() IsolationLevel
	// SetIsolation sets the isolation level for the transaction
	SetIsolation(level IsolationLevel) error
}

// KVStats provides statistics about the KV store
type KVStats struct {
	KeyCount        int64
	ApproximateSize int64
	MemTableSize    int64
	FlushCount      int64
	CompactionCount int64
	PendingWrites   int64
}

// Error types
var (
	ErrNotFound = &kvError{msg: "key not found"}
	ErrClosed   = &kvError{msg: "kv store closed"}
	ErrConflict = &kvError{msg: "transaction conflict"}
)

type kvError struct {
	msg string
}

func (e *kvError) Error() string {
	return e.msg
}

func IsNotFound(err error) bool {
	if err == nil {
		return false
	}
	if err == ErrNotFound {
		return true
	}
	if e, ok := err.(*kvError); ok {
		return e.msg == "key not found"
	}
	return false
}