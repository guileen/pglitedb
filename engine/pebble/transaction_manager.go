package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/engine/pebble/resources"
	"github.com/guileen/pglitedb/storage"
)

// Regular transaction implementation
type transaction struct {
	engine    *pebbleEngine
	kvTxn     storage.Transaction
	codec     codec.Codec
	isolation storage.IsolationLevel
}

// Compile-time check to ensure transaction implements engineTypes.Transaction interface
var _ engineTypes.Transaction = (*transaction)(nil)

// Snapshot transaction implementation
type snapshotTransaction struct {
	engine     *pebbleEngine
	snapshot   storage.Snapshot
	beginTS    int64
	mutations  map[string][]byte
	closed     bool
}

// Compile-time check to ensure snapshotTransaction implements engineTypes.Transaction interface
var _ engineTypes.Transaction = (*snapshotTransaction)(nil)

// BeginTx starts a new transaction with default isolation level
func (e *pebbleEngine) BeginTx(ctx context.Context) (engineTypes.Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	tx := &transaction{
		engine:    e,
		kvTxn:     kvTxn,
		codec:     e.codec,
		isolation: storage.ReadCommitted,
	}
	
	// Register transaction with deadlock detector
	if e.deadlockDetector != nil {
		txnID := getTransactionID(kvTxn)
		e.deadlockDetector.AddTransaction(txnID)
	}
	
	// Track transaction for leak detection
	rm := resources.GetResourceManager()
	rm.TrackTransaction(tx)
	
	// Set the isolation level on the transaction
	if err := tx.SetIsolation(storage.ReadCommitted); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}
	
	return tx, nil
}

// BeginTxWithIsolation starts a new transaction with specified isolation level
func (e *pebbleEngine) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	if level >= storage.RepeatableRead {
		return e.newSnapshotTx(ctx, level)
	}

	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if err := kvTxn.SetIsolation(level); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	tx := &transaction{
		engine:   e,
		kvTxn:    kvTxn,
		codec:    e.codec,
		isolation: level,
	}
	
	// Track transaction for leak detection
	rm := resources.GetResourceManager()
	rm.TrackTransaction(tx)
	
	// Set the isolation level on the transaction
	tx.SetIsolation(level)
	
	return tx, nil
}

// newSnapshotTx creates a new snapshot transaction
func (e *pebbleEngine) newSnapshotTx(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	snapshot, err := e.kv.NewSnapshot()
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	tx := &snapshotTransaction{
		engine:    e,
		snapshot:  snapshot,
		beginTS:   time.Now().UnixNano(),
		mutations: make(map[string][]byte),
		closed:    false,
	}
	
	// Track transaction for leak detection
	rm := resources.GetResourceManager()
	rm.TrackTransaction(tx)
	
	return tx, nil
}