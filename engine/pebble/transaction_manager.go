package pebble

import (
	"context"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
)

// Regular transaction implementation
type transaction struct {
	*BaseTransaction
	kvTxn storage.Transaction
	codec codec.Codec
}

// Snapshot transaction implementation
type snapshotTransaction struct {
	*BaseTransaction
	snapshot  storage.Snapshot
	beginTS   int64
	mutations map[string][]byte
	engine    *pebbleEngine
	closed    bool
}

// BeginTx starts a new transaction with default isolation level
func (e *pebbleEngine) BeginTx(ctx context.Context) (engineTypes.Transaction, error) {
	kvTxn, err := e.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	baseTx := NewBaseTransaction(e, storage.ReadCommitted)
	tx := &transaction{
		BaseTransaction: baseTx,
		kvTxn:           kvTxn,
		codec:           e.codec,
	}
	
	// Track transaction for leak detection
	rm := GetResourceManager()
	rm.TrackTransaction(tx)
	
	// Set the isolation level on the base transaction
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

	baseTx := NewBaseTransaction(e, level)
	tx := &transaction{
		BaseTransaction: baseTx,
		kvTxn:           kvTxn,
		codec:           e.codec,
	}
	
	// Track transaction for leak detection
	rm := GetResourceManager()
	rm.TrackTransaction(tx)
	
	// Set the isolation level on the base transaction
	tx.SetIsolation(level)
	
	return tx, nil
}

// newSnapshotTx creates a new snapshot transaction
func (e *pebbleEngine) newSnapshotTx(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	snapshot, err := e.kv.NewSnapshot()
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	baseTx := NewBaseTransaction(e, level)
	tx := &snapshotTransaction{
		BaseTransaction: baseTx,
		snapshot:        snapshot,
		beginTS:         time.Now().UnixNano(),
		mutations:       make(map[string][]byte),
		engine:          e,
		closed:          false,
	}
	
	// Track transaction for leak detection
	rm := GetResourceManager()
	rm.TrackTransaction(tx)
	
	return tx, nil
}