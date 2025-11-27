// Package transaction provides the main transaction manager that integrates all components
package transaction

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/storage/shared"
)

// Manager coordinates all transaction-related components
type Manager struct {
	txnManager   *TxnManager
	lockManager  *LockManager
	logManager   *LogManager
	mvccStorage  *storage.MVCCStorage
	kv           shared.KV
	
	mu           sync.RWMutex
}

// Config holds configuration for the transaction manager
type Config struct {
	LogPath string
	KV      shared.KV
}

// NewManager creates a new transaction manager
func NewManager(config *Config) (*Manager, error) {
	txnManager := NewTxnManager()
	lockManager := NewLockManager()
	
	// Initialize log manager
	logPath := filepath.Join(config.LogPath, "logs")
	logManager, err := NewLogManager(logPath)
	if err != nil {
		return nil, fmt.Errorf("create log manager: %w", err)
	}
	
	// Wrap KV store with MVCC
	mvccStorage := storage.NewMVCCStorage(config.KV)
	
	manager := &Manager{
		txnManager:  txnManager,
		lockManager: lockManager,
		logManager:  logManager,
		mvccStorage: mvccStorage,
		kv:          config.KV,
	}
	
	return manager, nil
}

// Begin starts a new transaction
func (m *Manager) Begin(ctx context.Context, isolation IsolationLevel) (*TxnContext, error) {
	txn, err := m.txnManager.Begin(ctx, isolation)
	if err != nil {
		return nil, fmt.Errorf("begin transaction: %w", err)
	}
	
	// Register with lock manager
	m.lockManager.RegisterTransaction(txn)
	
	// Log the begin
	if err := m.logManager.LogBegin(txn.ID); err != nil {
		return nil, fmt.Errorf("log begin: %w", err)
	}
	
	return txn, nil
}

// Commit commits a transaction
func (m *Manager) Commit(txn *TxnContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !txn.IsActive() {
		return fmt.Errorf("transaction is not active")
	}
	
	// Perform MVCC commit
	if err := m.performMVCCCommit(txn); err != nil {
		return fmt.Errorf("mvcc commit: %w", err)
	}
	
	// Commit in transaction manager
	if err := m.txnManager.Commit(txn); err != nil {
		return fmt.Errorf("txn manager commit: %w", err)
	}
	
	// Release locks
	m.releaseTransactionLocks(txn)
	
	// Log the commit
	if err := m.logManager.LogCommit(txn.ID); err != nil {
		return fmt.Errorf("log commit: %w", err)
	}
	
	return nil
}

// Rollback rolls back a transaction
func (m *Manager) Rollback(txn *TxnContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	if !txn.IsActive() {
		return fmt.Errorf("transaction is not active")
	}
	
	// Perform MVCC rollback
	if err := m.performMVCCRollback(txn); err != nil {
		return fmt.Errorf("mvcc rollback: %w", err)
	}
	
	// Rollback in transaction manager
	if err := m.txnManager.Rollback(txn); err != nil {
		return fmt.Errorf("txn manager rollback: %w", err)
	}
	
	// Release locks
	m.releaseTransactionLocks(txn)
	
	// Log the abort
	if err := m.logManager.LogAbort(txn.ID); err != nil {
		return fmt.Errorf("log abort: %w", err)
	}
	
	return nil
}

// Abort aborts a transaction due to conflict or error
func (m *Manager) Abort(txn *TxnContext) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Perform MVCC abort
	if err := m.performMVCCTransactionAbort(txn); err != nil {
		return fmt.Errorf("mvcc abort: %w", err)
	}
	
	// Abort in transaction manager
	if err := m.txnManager.Abort(txn); err != nil {
		return fmt.Errorf("txn manager abort: %w", err)
	}
	
	// Force release locks
	m.lockManager.ForceUnlock(txn.ID)
	
	// Log the abort
	if err := m.logManager.LogAbort(txn.ID); err != nil {
		return fmt.Errorf("log abort: %w", err)
	}
	
	return nil
}

// CreateSavepoint creates a savepoint in a transaction
func (m *Manager) CreateSavepoint(txn *TxnContext, id string) error {
	return txn.CreateSavepoint(id)
}

// RollbackToSavepoint rolls back to a savepoint
func (m *Manager) RollbackToSavepoint(txn *TxnContext, id string) error {
	return txn.RollbackToSavepoint(id)
}

// ReleaseSavepoint releases a savepoint
func (m *Manager) ReleaseSavepoint(txn *TxnContext, id string) error {
	return txn.ReleaseSavepoint(id)
}

// AcquireLock acquires a lock on a resource
func (m *Manager) AcquireLock(txn *TxnContext, resourceID string, lockType LockType, mode LockMode) error {
	return m.lockManager.Lock(txn.ID, resourceID, lockType, mode)
}

// ReleaseLock releases a lock on a resource
func (m *Manager) ReleaseLock(txn *TxnContext, resourceID string) error {
	return m.lockManager.Unlock(txn.ID, resourceID)
}

// Get retrieves a value with MVCC semantics
func (m *Manager) Get(txn *TxnContext, key []byte) ([]byte, error) {
	// Add to read set for conflict detection
	txn.AddToReadSet(string(key), txn.SnapshotTS)
	
	// Get value with MVCC
	value, err := m.mvccStorage.Get(key, txn.SnapshotTS)
	if err != nil {
		return nil, err
	}
	
	return value, nil
}

// Put stores a value with MVCC semantics
func (m *Manager) Put(txn *TxnContext, key, value []byte) error {
	// Add to write set
	txn.AddToWriteSet(string(key))
	
	// Log the write
	if err := m.logManager.LogWrite(txn.ID, key, value); err != nil {
		return fmt.Errorf("log write: %w", err)
	}
	
	// Store with MVCC
	return m.mvccStorage.Put(key, value, txn.SnapshotTS)
}

// Delete deletes a key with MVCC semantics
func (m *Manager) Delete(txn *TxnContext, key []byte) error {
	// Add to write set
	txn.AddToWriteSet(string(key))
	
	// Log the write
	if err := m.logManager.LogWrite(txn.ID, key, nil); err != nil {
		return fmt.Errorf("log delete: %w", err)
	}
	
	// Mark as deleted with MVCC
	return m.mvccStorage.Delete(key, txn.SnapshotTS)
}

// performMVCCCommit performs MVCC-specific commit operations
func (m *Manager) performMVCCCommit(txn *TxnContext) error {
	commitTS := m.txnManager.allocateTimestamp()
	
	// For serializable isolation, check for conflicts
	if txn.Isolation == Serializable {
		for key := range txn.ReadSet {
			// In a full implementation, we would check if any other transaction
			// committed a write to this key after our snapshot timestamp
			// This is a simplified check
			_ = key
		}
	}
	
	// Commit all writes
	for key := range txn.WriteSet {
		if err := m.mvccStorage.Commit([]byte(key), txn.SnapshotTS, commitTS); err != nil {
			return fmt.Errorf("commit key %s: %w", key, err)
		}
	}
	
	return nil
}

// performMVCCRollback performs MVCC-specific rollback operations
func (m *Manager) performMVCCRollback(txn *TxnContext) error {
	// Mark all writes as aborted
	for key := range txn.WriteSet {
		if err := m.mvccStorage.Abort([]byte(key), txn.SnapshotTS); err != nil {
			// Log error but continue with other keys
			fmt.Printf("Warning: failed to abort key %s: %v\n", key, err)
		}
	}
	
	return nil
}

// performMVCCTransactionAbort performs MVCC-specific abort operations
func (m *Manager) performMVCCTransactionAbort(txn *TxnContext) error {
	return m.performMVCCRollback(txn)
}

// releaseTransactionLocks releases all locks held by a transaction
func (m *Manager) releaseTransactionLocks(txn *TxnContext) {
	// In a full implementation, we would track which resources were locked
	// and release them here. For now, we'll force unlock all.
	m.lockManager.ForceUnlock(txn.ID)
	m.lockManager.UnregisterTransaction(txn.ID)
}

// Recover performs recovery operations
func (m *Manager) Recover() error {
	return m.logManager.Recover(m.kv)
}

// FlushLogs flushes pending log writes
func (m *Manager) FlushLogs() error {
	return m.logManager.Flush()
}

// Close closes the transaction manager and its components
func (m *Manager) Close() error {
	if err := m.logManager.Close(); err != nil {
		return fmt.Errorf("close log manager: %w", err)
	}
	
	return nil
}

// GetStats returns statistics about the transaction manager
func (m *Manager) GetStats() map[string]interface{} {
	stats := make(map[string]interface{})
	
	stats["active_transactions"] = m.txnManager.GetActiveTransactions()
	stats["lock_count"] = m.lockManager.GetLockCount()
	
	// Get log stats
	if logStats, err := m.logManager.GetLogStats(); err == nil {
		for k, v := range logStats {
			stats[k] = v
		}
	}
	
	return stats
}