// Package storage provides the public API for the storage module
package storage

import (
	"github.com/guileen/pglitedb/storage/internal/kv"
	"github.com/guileen/pglitedb/storage/shared"
)

// WriteOptions defines options for write operations
type WriteOptions = shared.WriteOptions

// IsolationLevel defines the isolation level for transactions
type IsolationLevel = shared.IsolationLevel

const (
	// ReadUncommitted allows reading uncommitted changes from other transactions
	ReadUncommitted IsolationLevel = shared.ReadUncommitted
	// ReadCommitted ensures that only committed data can be read
	ReadCommitted IsolationLevel = shared.ReadCommitted
	// RepeatableRead ensures that data read in a transaction remains consistent
	RepeatableRead IsolationLevel = shared.RepeatableRead
	// SnapshotIsolation provides a consistent snapshot of the database at transaction start
	SnapshotIsolation IsolationLevel = shared.SnapshotIsolation
	// Serializable provides the highest isolation level, preventing all anomalies
	Serializable IsolationLevel = shared.Serializable
)

var (
	DefaultWriteOptions = shared.DefaultWriteOptions
	SyncWriteOptions    = shared.SyncWriteOptions
)

// KV defines the interface for key-value storage operations
type KV = shared.KV

// Batch defines the interface for batch operations
type Batch = shared.Batch

// IteratorOptions defines options for iterator operations
type IteratorOptions = shared.IteratorOptions

// Iterator defines the interface for iterating over key-value pairs
type Iterator = shared.Iterator

// Snapshot defines the interface for snapshot operations
type Snapshot = shared.Snapshot

// Transaction defines the interface for transaction operations
type Transaction = shared.Transaction

// KVStats provides statistics about the KV store
type KVStats = shared.KVStats

// Error types
var (
	ErrNotFound = shared.ErrNotFound
	ErrClosed   = shared.ErrClosed
	ErrConflict = shared.ErrConflict
)

// IsNotFound checks if an error is a "not found" error
func IsNotFound(err error) bool {
	return shared.IsNotFound(err)
}

// PebbleConfig holds the configuration for the Pebble KV store
type PebbleConfig = kv.PebbleConfig

// NewPebbleKV creates a new Pebble-based KV store
func NewPebbleKV(config *PebbleConfig) (KV, error) {
	return kv.NewPebbleKV(config)
}

// DefaultPebbleConfig creates a default configuration for Pebble KV store
func DefaultPebbleConfig(path string) *PebbleConfig {
	return kv.DefaultPebbleConfig(path)
}

// TestOptimizedPebbleConfig creates a configuration optimized for testing performance
func TestOptimizedPebbleConfig(path string) *PebbleConfig {
	return kv.TestOptimizedPebbleConfig(path)
}

// PostgreSQLOptimizedPebbleConfig creates a configuration optimized for PostgreSQL-like workloads
func PostgreSQLOptimizedPebbleConfig(path string) *PebbleConfig {
	return kv.PostgreSQLOptimizedPebbleConfig(path)
}