package transactions

import (
	"context"
	"fmt"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/codec"
)

// TransactionManager manages transaction creation and lifecycle
type TransactionManager struct {
	engine engineTypes.StorageEngine
	kv     storage.KV
	codec  codec.Codec
}

// NewTransactionManager creates a new transaction manager
func NewTransactionManager(engine engineTypes.StorageEngine, kv storage.KV, codec codec.Codec) *TransactionManager {
	return &TransactionManager{
		engine: engine,
		kv:     kv,
		codec:  codec,
	}
}

// BeginTx starts a new transaction with default isolation level
func (tm *TransactionManager) BeginTx(ctx context.Context) (engineTypes.Transaction, error) {
	kvTxn, err := tm.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	tx := NewRegularTransaction(tm.engine, kvTxn, tm.codec)

	// Track transaction for leak detection
	// TODO: Implement resource manager tracking

	// Set the isolation level on the transaction
	if err := tx.SetIsolation(storage.ReadCommitted); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	return tx, nil
}

// BeginTxWithIsolation starts a new transaction with specified isolation level
func (tm *TransactionManager) BeginTxWithIsolation(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	if level >= storage.RepeatableRead {
		return tm.newSnapshotTx(ctx, level)
	}

	kvTxn, err := tm.kv.NewTransaction(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}

	if err := kvTxn.SetIsolation(level); err != nil {
		kvTxn.Rollback()
		return nil, fmt.Errorf("set isolation level: %w", err)
	}

	tx := NewRegularTransaction(tm.engine, kvTxn, tm.codec)

	// Track transaction for leak detection
	// TODO: Implement resource manager tracking

	// Set the isolation level on the transaction
	tx.SetIsolation(level)

	return tx, nil
}

// newSnapshotTx creates a new snapshot transaction
func (tm *TransactionManager) newSnapshotTx(ctx context.Context, level storage.IsolationLevel) (engineTypes.Transaction, error) {
	snapshot, err := tm.kv.NewSnapshot()
	if err != nil {
		return nil, fmt.Errorf("create snapshot: %w", err)
	}

	tx := NewSnapshotTransaction(tm.engine, snapshot)

	// Track transaction for leak detection
	// TODO: Implement resource manager tracking

	return tx, nil
}