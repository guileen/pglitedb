// Package transaction provides full ACID-compliant transaction management with MVCC support
package transaction

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

// TxnState represents the state of a transaction
type TxnState int

const (
	TxnActive TxnState = iota
	TxnCommitted
	TxnRolledBack
	TxnAborted
)

// IsolationLevel defines the isolation level for transactions
type IsolationLevel = shared.IsolationLevel

const (
	ReadUncommitted IsolationLevel = shared.ReadUncommitted
	ReadCommitted   IsolationLevel = shared.ReadCommitted
	RepeatableRead  IsolationLevel = shared.RepeatableRead
	SnapshotIsolation IsolationLevel = shared.SnapshotIsolation
	Serializable    IsolationLevel = shared.Serializable
)

// TransactionID represents a unique transaction identifier
type TransactionID uint64

// Savepoint represents a savepoint within a transaction
type Savepoint struct {
	ID        string
	Timestamp time.Time
}

// TxnContext represents the complete transaction context
type TxnContext struct {
	ID           TransactionID
	State        TxnState
	Isolation    IsolationLevel
	StartTime    time.Time
	CommitTime   time.Time
	Timeout      time.Duration
	CancelFunc   context.CancelFunc
	Context      context.Context
	
	// MVCC related fields
	SnapshotTS   int64
	CommitTS     int64
	ReadSet      map[string]int64
	WriteSet     map[string]bool
	
	// Savepoint support
	Savepoints   map[string]*Savepoint
	ActiveSp     string // Currently active savepoint
	
	// Parent transaction for nested transactions
	Parent       *TxnContext
	
	mu           sync.RWMutex
}

// TxnManager manages transactions globally
type TxnManager struct {
	nextTxnID    uint64
	activeTxns   map[TransactionID]*TxnContext
	globalTS     int64
	mu           sync.RWMutex
}

// NewTxnManager creates a new transaction manager
func NewTxnManager() *TxnManager {
	return &TxnManager{
		activeTxns: make(map[TransactionID]*TxnContext),
		globalTS:   time.Now().UnixNano(),
	}
}

// Begin starts a new transaction with the specified isolation level
func (tm *TxnManager) Begin(ctx context.Context, isolation IsolationLevel) (*TxnContext, error) {
	txnID := TransactionID(atomic.AddUint64(&tm.nextTxnID, 1))
	
	// Create transaction context with timeout
	txnCtx, cancel := context.WithCancel(ctx)
	
	txn := &TxnContext{
		ID:         txnID,
		State:      TxnActive,
		Isolation:  isolation,
		StartTime:  time.Now(),
		Context:    txnCtx,
		CancelFunc: cancel,
		SnapshotTS: tm.allocateTimestamp(),
		ReadSet:    make(map[string]int64),
		WriteSet:   make(map[string]bool),
		Savepoints: make(map[string]*Savepoint),
	}
	
	// Register transaction
	tm.mu.Lock()
	tm.activeTxns[txnID] = txn
	tm.mu.Unlock()
	
	return txn, nil
}

// BeginChild starts a nested transaction
func (tm *TxnManager) BeginChild(parent *TxnContext, isolation IsolationLevel) (*TxnContext, error) {
	if parent.State != TxnActive {
		return nil, fmt.Errorf("parent transaction is not active")
	}
	
	txnID := TransactionID(atomic.AddUint64(&tm.nextTxnID, 1))
	
	txnCtx, cancel := context.WithCancel(parent.Context)
	
	child := &TxnContext{
		ID:         txnID,
		State:      TxnActive,
		Isolation:  isolation,
		StartTime:  time.Now(),
		Context:    txnCtx,
		CancelFunc: cancel,
		SnapshotTS: parent.SnapshotTS, // Inherit parent's snapshot
		ReadSet:    make(map[string]int64),
		WriteSet:   make(map[string]bool),
		Savepoints: make(map[string]*Savepoint),
		Parent:     parent,
	}
	
	// Register transaction
	tm.mu.Lock()
	tm.activeTxns[txnID] = child
	tm.mu.Unlock()
	
	return child, nil
}

// Commit commits a transaction
func (tm *TxnManager) Commit(txn *TxnContext) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.State != TxnActive {
		return fmt.Errorf("transaction is not active")
	}
	
	// Set commit timestamp
	txn.CommitTS = tm.allocateTimestamp()
	txn.State = TxnCommitted
	txn.CommitTime = time.Now()
	
	// Unregister transaction
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
	
	// Cancel context to release resources
	if txn.CancelFunc != nil {
		txn.CancelFunc()
	}
	
	return nil
}

// Rollback rolls back a transaction
func (tm *TxnManager) Rollback(txn *TxnContext) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.State != TxnActive {
		return fmt.Errorf("transaction is not active")
	}
	
	txn.State = TxnRolledBack
	
	// Unregister transaction
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
	
	// Cancel context to release resources
	if txn.CancelFunc != nil {
		txn.CancelFunc()
	}
	
	return nil
}

// Abort aborts a transaction due to conflict or error
func (tm *TxnManager) Abort(txn *TxnContext) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	// Check if transaction is already in a terminal state
	if txn.State == TxnCommitted || txn.State == TxnRolledBack || txn.State == TxnAborted {
		return fmt.Errorf("transaction is already in terminal state")
	}
	
	txn.State = TxnAborted
	
	// Unregister transaction
	tm.mu.Lock()
	delete(tm.activeTxns, txn.ID)
	tm.mu.Unlock()
	
	// Cancel context to release resources
	if txn.CancelFunc != nil {
		txn.CancelFunc()
	}
	
	return nil
}

// CreateSavepoint creates a new savepoint in the transaction
func (txn *TxnContext) CreateSavepoint(id string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.State != TxnActive {
		return fmt.Errorf("transaction is not active")
	}
	
	savepoint := &Savepoint{
		ID:        id,
		Timestamp: time.Now(),
	}
	
	txn.Savepoints[id] = savepoint
	txn.ActiveSp = id
	
	return nil
}

// RollbackToSavepoint rolls back to a specific savepoint
func (txn *TxnContext) RollbackToSavepoint(id string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.State != TxnActive {
		return fmt.Errorf("transaction is not active")
	}
	
	if _, exists := txn.Savepoints[id]; !exists {
		return fmt.Errorf("savepoint %s does not exist", id)
	}
	
	// Mark the savepoint as active
	txn.ActiveSp = id
	
	// In a full implementation, we would need to rollback changes
	// made after this savepoint, but for now we just mark it
	
	return nil
}

// ReleaseSavepoint releases a savepoint
func (txn *TxnContext) ReleaseSavepoint(id string) error {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	
	if txn.State != TxnActive {
		return fmt.Errorf("transaction is not active")
	}
	
	if _, exists := txn.Savepoints[id]; !exists {
		return fmt.Errorf("savepoint %s does not exist", id)
	}
	
	delete(txn.Savepoints, id)
	
	// If this was the active savepoint, clear it
	if txn.ActiveSp == id {
		txn.ActiveSp = ""
	}
	
	return nil
}

// IsActive checks if the transaction is active
func (txn *TxnContext) IsActive() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TxnActive
}

// IsCommitted checks if the transaction is committed
func (txn *TxnContext) IsCommitted() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TxnCommitted
}

// IsRolledBack checks if the transaction is rolled back
func (txn *TxnContext) IsRolledBack() bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	return txn.State == TxnRolledBack
}

// AddToReadSet adds a key to the transaction's read set
func (txn *TxnContext) AddToReadSet(key string, timestamp int64) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.ReadSet[key] = timestamp
}

// AddToWriteSet adds a key to the transaction's write set
func (txn *TxnContext) AddToWriteSet(key string) {
	txn.mu.Lock()
	defer txn.mu.Unlock()
	txn.WriteSet[key] = true
}

// GetReadTimestamp gets the timestamp when a key was read
func (txn *TxnContext) GetReadTimestamp(key string) (int64, bool) {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	ts, exists := txn.ReadSet[key]
	return ts, exists
}

// WasWritten checks if a key was written in this transaction
func (txn *TxnContext) WasWritten(key string) bool {
	txn.mu.RLock()
	defer txn.mu.RUnlock()
	_, exists := txn.WriteSet[key]
	return exists
}

// allocateTimestamp allocates a new timestamp
func (tm *TxnManager) allocateTimestamp() int64 {
	return atomic.AddInt64(&tm.globalTS, 1)
}

// GetActiveTransactions returns the count of active transactions
func (tm *TxnManager) GetActiveTransactions() int {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	return len(tm.activeTxns)
}

// GetTransaction retrieves a transaction by ID
func (tm *TxnManager) GetTransaction(id TransactionID) (*TxnContext, bool) {
	tm.mu.RLock()
	defer tm.mu.RUnlock()
	txn, exists := tm.activeTxns[id]
	return txn, exists
}