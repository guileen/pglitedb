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
	txnID      uint64
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
}

func (t *PebbleTransaction) Get(key []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil, shared.ErrClosed
	}

	// For ReadUncommitted isolation, we can read uncommitted changes
	// First check if this key was written in this transaction
	value, closer, err := t.batch.Get(key)
	if err == nil {
		defer closer.Close()
		result := make([]byte, len(value))
		copy(result, value)

		if t.readKeys == nil {
			t.readKeys = make(map[string][]byte)
		}
		t.readKeys[string(key)] = result

		return result, nil
	}

	// For all isolation levels, check the database for committed data
	// This ensures visibility of data inserted via engine.InsertRow
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

	if t.readKeys == nil {
		t.readKeys = make(map[string][]byte)
	}
	t.readKeys[string(key)] = result

	if t.isolation == shared.Serializable {
		if t.readSet == nil {
			t.readSet = make(map[string]int64)
		}
		t.readSet[string(key)] = t.kv.getKeyTimestamp(key)
	}

	return result, nil
}

func (t *PebbleTransaction) Set(key, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	// Track the written key
	t.writeKeys[string(key)] = true

	return t.batch.Set(key, value, nil)
}

func (t *PebbleTransaction) Delete(key []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	// Track the written key
	t.writeKeys[string(key)] = true

	return t.batch.Delete(key, nil)
}

func (t *PebbleTransaction) NewIterator(opts *shared.IteratorOptions) shared.Iterator {
	var pebbleOpts *pebble.IterOptions
	if opts != nil {
		pebbleOpts = &pebble.IterOptions{
			LowerBound: opts.LowerBound,
			UpperBound: opts.UpperBound,
		}
	}

	// Create an iterator that combines both the batch and the database
	// This is a simplified approach - in a real implementation, we would need
	// to merge the iterators properly to handle conflicts

	// For now, if the batch is empty, just use the database iterator
	if t.batch.Count() == 0 {
		iter, err := t.db.NewIter(pebbleOpts)
		if err != nil {
			return nil
		}
		if iter == nil {
			return nil
		}

		return &PebbleIterator{
			iter:    iter,
			reverse: opts != nil && opts.Reverse,
			err:     err,
		}
	}

	// If the batch has operations, we need to handle this more carefully
	// For now, we'll use the batch iterator, but note that this might not
	// include all the data that exists in the database
	iter, err := t.batch.NewIter(pebbleOpts)
	if err != nil {
		return nil
	}
	if iter == nil {
		return nil
	}

	return &PebbleIterator{
		iter:    iter,
		reverse: opts != nil && opts.Reverse,
		err:     err,
	}
}

func (t *PebbleTransaction) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	if t.isolation == shared.Serializable {
		for key, readTS := range t.readSet {
			currentTS := t.kv.getKeyTimestamp([]byte(key))
			if currentTS > readTS {
				t.closed = true
				t.kv.activeTransactions.Delete(t.txnID)
				t.batch.Close()
				return shared.ErrConflict
			}
		}
	}

	t.commitTS = t.kv.allocateTimestamp()

	for key := range t.writeKeys {
		t.kv.setKeyTimestamp([]byte(key), t.commitTS)
	}

	t.closed = true

	t.kv.activeTransactions.Delete(t.txnID)

	if err := t.batch.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("transaction commit: %w", err)
	}
	return nil
}

func (t *PebbleTransaction) Rollback() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return nil
	}

	t.closed = true

	// Unregister the transaction
	t.kv.activeTransactions.Delete(t.txnID)

	return t.batch.Close()
}

func (t *PebbleTransaction) Isolation() shared.IsolationLevel {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.isolation
}

func (t *PebbleTransaction) SetIsolation(level shared.IsolationLevel) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed {
		return shared.ErrClosed
	}

	// Validate the isolation level
	switch level {
	case shared.ReadUncommitted, shared.ReadCommitted, shared.RepeatableRead, shared.SnapshotIsolation, shared.Serializable:
		t.isolation = level
		return nil
	default:
		return fmt.Errorf("invalid isolation level: %d", level)
	}
}

func (t *PebbleTransaction) TxnID() uint64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.txnID
}

func (t *PebbleTransaction) Close() error {
	return t.Rollback()
}