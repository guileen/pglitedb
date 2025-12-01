package kv

import (
	"fmt"
	"sync"

	"github.com/cockroachdb/pebble"
	"github.com/guileen/pglitedb/storage/shared"
)

type PebbleTransaction struct {
	db         *pebble.DB
	batch      *pebble.Batch
	mu         sync.RWMutex
	closed     bool
	readKeys   map[string][]byte
	isolation  shared.IsolationLevel
	txnID      uint64 // Atomic access - never modified after creation
	kv         *PebbleKV
	writeKeys  map[string]bool

	startTS    int64
	commitTS   int64
	readSet    map[string]int64
	
	// Additional fields for transaction state
	reads      map[string]struct{}
	writes     map[string][]byte
	committed  bool
	rolledBack bool
	readOnly   bool
	
	// Add separate mutex for readKeys to reduce contention
	readKeysMu sync.RWMutex
	// Add separate mutex for writeKeys to reduce contention
	writeKeysMu sync.RWMutex
	// Add separate mutex for readSet to reduce contention
	readSetMu sync.RWMutex
}

func (t *PebbleTransaction) Get(key []byte) ([]byte, error) {
	// First check if this key was written in this transaction (no lock needed for batch.Get)
	value, closer, err := t.batch.Get(key)
	if err == nil {
		defer closer.Close()
		result := make([]byte, len(value))
		copy(result, value)
		
		// Only lock readKeys when we need to modify it
		t.readKeysMu.Lock()
		if t.readKeys == nil {
			t.readKeys = make(map[string][]byte)
		}
		t.readKeys[string(key)] = result
		t.readKeysMu.Unlock()

		return result, nil
	}

	// Check if transaction is closed (minimal lock scope)
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil, shared.ErrClosed
	}
	t.mu.RUnlock()

	// For all isolation levels, check the database for committed data
	value, closer, err = t.db.Get(key)
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, shared.ErrNotFound
		}
		return nil, fmt.Errorf("transaction get: %w", err)
	}
	defer closer.Close()

	result := make([]byte, len(value))
	copy(result, value)

	// Handle serializable isolation separately
	if t.isolation == shared.Serializable {
		timestamp := t.kv.getKeyTimestamp(key)
		
		// Only lock when we need to modify readKeys and readSet
		t.readKeysMu.Lock()
		if t.readKeys == nil {
			t.readKeys = make(map[string][]byte)
		}
		t.readKeys[string(key)] = result
		t.readKeysMu.Unlock()
		
		t.readSetMu.Lock()
		if t.readSet == nil {
			t.readSet = make(map[string]int64)
		}
		t.readSet[string(key)] = timestamp
		t.readSetMu.Unlock()
	} else {
		// Only lock when we need to modify readKeys
		t.readKeysMu.Lock()
		if t.readKeys == nil {
			t.readKeys = make(map[string][]byte)
		}
		t.readKeys[string(key)] = result
		t.readKeysMu.Unlock()
	}

	return result, nil
}

func (t *PebbleTransaction) Set(key, value []byte) error {
	// Check if transaction is closed with minimal lock scope
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return shared.ErrClosed
	}
	t.mu.RUnlock()

	// Track the written key with separate mutex
	t.writeKeysMu.Lock()
	if t.writeKeys == nil {
		t.writeKeys = make(map[string]bool)
	}
	t.writeKeys[string(key)] = true
	t.writeKeysMu.Unlock()

	return t.batch.Set(key, value, nil)
}

func (t *PebbleTransaction) Delete(key []byte) error {
	// Check if transaction is closed with minimal lock scope
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return shared.ErrClosed
	}
	t.mu.RUnlock()

	// Track the written key with separate mutex
	t.writeKeysMu.Lock()
	if t.writeKeys == nil {
		t.writeKeys = make(map[string]bool)
	}
	t.writeKeys[string(key)] = true
	t.writeKeysMu.Unlock()

	return t.batch.Delete(key, nil)
}

func (t *PebbleTransaction) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	// Create a merging iterator that properly combines database and batch data
	iter, err := NewMergingIterator(t.db, t.batch, opts)
	if err != nil {
		return nil
	}
	return iter
}

func (t *PebbleTransaction) Commit() error {
	// Check if transaction is closed with minimal lock scope
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return shared.ErrClosed
	}
	t.mu.RUnlock()

	// For serializable isolation, check for conflicts with separate locks
	// but with a limit to prevent long scans
	if t.isolation == shared.Serializable {
		t.readSetMu.RLock()
		checkCount := 0
		maxChecks := 1000 // Limit to prevent performance issues
		conflictDetected := false
		
		for key, readTS := range t.readSet {
			if checkCount >= maxChecks {
				break // Limit reached
			}
			checkCount++
			
			currentTS := t.kv.getKeyTimestamp([]byte(key))
			if currentTS > readTS {
				conflictDetected = true
				break
			}
		}
		t.readSetMu.RUnlock()
		
		if conflictDetected {
			// Set closed flag and unregister transaction
			t.mu.Lock()
			t.closed = true
			t.mu.Unlock()
			t.kv.activeTransactions.Delete(t.txnID)
			t.batch.Close()
			return shared.ErrConflict
		}
	}

	t.commitTS = t.kv.allocateTimestamp()

	// Update timestamps for written keys with batch operation
	// Collect keys first to minimize lock time
	var keysToTimestamp [][]byte
	t.writeKeysMu.RLock()
	keysToTimestamp = make([][]byte, 0, len(t.writeKeys))
	for key := range t.writeKeys {
		keysToTimestamp = append(keysToTimestamp, []byte(key))
	}
	t.writeKeysMu.RUnlock()

	// Update timestamps in batch
	for _, key := range keysToTimestamp {
		t.kv.setKeyTimestamp(key, t.commitTS)
	}

	// Set closed flag and unregister transaction with minimal lock scope
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.kv.activeTransactions.Delete(t.txnID)

	if err := t.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("transaction commit: %w", err)
	}
	return nil
}

func (t *PebbleTransaction) Rollback() error {
	// Check if transaction is closed with minimal lock scope
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return nil
	}
	t.mu.RUnlock()

	// Set closed flag and unregister transaction with minimal lock scope
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.kv.activeTransactions.Delete(t.txnID)

	return t.batch.Close()
}

func (t *PebbleTransaction) Isolation() shared.IsolationLevel {
	// Direct access to isolation level with minimal lock scope
	t.mu.RLock()
	isolation := t.isolation
	t.mu.RUnlock()
	return isolation
}

func (t *PebbleTransaction) SetIsolation(level shared.IsolationLevel) error {
	// Check if transaction is closed with minimal lock scope
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return shared.ErrClosed
	}
	t.mu.RUnlock()

	// Validate the isolation level
	switch level {
	case shared.ReadUncommitted, shared.ReadCommitted, shared.RepeatableRead, shared.SnapshotIsolation, shared.Serializable:
		// Set isolation level with minimal lock scope
		t.mu.Lock()
		t.isolation = level
		t.mu.Unlock()
		return nil
	default:
		return fmt.Errorf("invalid isolation level: %d", level)
	}
}

func (t *PebbleTransaction) TxnID() uint64 {
	// txnID is immutable after creation, so we can return it directly
	// This avoids the mutex lock and reduces synchronization overhead
	return t.txnID
}

func (t *PebbleTransaction) Close() error {
	return t.Rollback()
}