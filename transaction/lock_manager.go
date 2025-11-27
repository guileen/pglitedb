// Package transaction provides lock management for concurrency control
package transaction

import (
	"fmt"
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage/shared"
)

// LockType represents the type of lock
type LockType int

const (
	LockNone LockType = iota
	LockShared
	LockExclusive
)

// LockMode represents the mode of lock acquisition
type LockMode int

const (
	LockModeBlock LockMode = iota
	LockModeNonBlock
	LockModeTry
)

// Lock represents a lock on a resource
type Lock struct {
	ResourceID string
	LockType   LockType
	Holder     TransactionID
	Waiters    []TransactionID
}

// DeadlockInfo contains information about a detected deadlock
type DeadlockInfo struct {
	Victim     TransactionID
	WaitChain  []TransactionID
	Timestamp  time.Time
}

// LockManager manages locks for transactions
type LockManager struct {
	locks        map[string]*Lock
	waiters      map[TransactionID][]string // Resources a transaction is waiting for
	transactions map[TransactionID]*TxnContext
	detector     *DeadlockDetector
	mu           sync.RWMutex
}

// DeadlockDetector detects and resolves deadlocks
type DeadlockDetector struct {
	lockManager *LockManager
	timeout     time.Duration
}

// NewLockManager creates a new lock manager
func NewLockManager() *LockManager {
	lm := &LockManager{
		locks:        make(map[string]*Lock),
		waiters:      make(map[TransactionID][]string),
		transactions: make(map[TransactionID]*TxnContext),
	}
	
	lm.detector = &DeadlockDetector{
		lockManager: lm,
		timeout:     5 * time.Second, // Default timeout
	}
	
	return lm
}

// SetDeadlockTimeout sets the timeout for deadlock detection
func (lm *LockManager) SetDeadlockTimeout(timeout time.Duration) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.detector.timeout = timeout
}

// RegisterTransaction registers a transaction with the lock manager
func (lm *LockManager) RegisterTransaction(txn *TxnContext) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	lm.transactions[txn.ID] = txn
}

// UnregisterTransaction unregisters a transaction from the lock manager
func (lm *LockManager) UnregisterTransaction(txnID TransactionID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	delete(lm.transactions, txnID)
	
	// Remove from waiters
	delete(lm.waiters, txnID)
}

// Lock acquires a lock on a resource
func (lm *LockManager) Lock(txnID TransactionID, resourceID string, lockType LockType, mode LockMode) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// Check if transaction exists
	txn, exists := lm.transactions[txnID]
	if !exists {
		return fmt.Errorf("transaction %d not registered", txnID)
	}
	
	// Check if transaction is active
	if !txn.IsActive() {
		return fmt.Errorf("transaction %d is not active", txnID)
	}
	
	lock, exists := lm.locks[resourceID]
	if !exists {
		// No existing lock, create new one
		lm.locks[resourceID] = &Lock{
			ResourceID: resourceID,
			LockType:   lockType,
			Holder:     txnID,
		}
		return nil
	}
	
	// Check if this transaction already holds the lock
	if lock.Holder == txnID {
		// Upgrade lock if needed
		if lockType == LockExclusive && lock.LockType == LockShared {
			lock.LockType = LockExclusive
		}
		return nil
	}
	
	// Check lock compatibility
	if lm.isCompatible(lock, lockType) {
		// Compatible lock, grant it
		if lockType == LockExclusive {
			// Upgrade to exclusive if needed
			lock.LockType = LockExclusive
		}
		lock.Holder = txnID
		return nil
	}
	
	// Lock is not compatible, handle based on mode
	switch mode {
	case LockModeNonBlock:
		return shared.ErrConflict
	case LockModeTry:
		return shared.ErrConflict
	case LockModeBlock:
		// Add to waiters
		lock.Waiters = append(lock.Waiters, txnID)
		lm.waiters[txnID] = append(lm.waiters[txnID], resourceID)
		
		// Check for deadlock
		if lm.detector.detectDeadlock(txnID, resourceID) {
			// Resolve deadlock by aborting this transaction
			if txn, exists := lm.transactions[txnID]; exists {
				// Mark as aborted
				txn.State = TxnAborted
				// Remove from waiters
				delete(lm.waiters, txnID)
				// Remove from lock waiters
				for i, waiter := range lock.Waiters {
					if waiter == txnID {
						lock.Waiters = append(lock.Waiters[:i], lock.Waiters[i+1:]...)
						break
					}
				}
			}
			return fmt.Errorf("deadlock detected, transaction aborted")
		}
		
		// Wait for lock (in a real implementation, this would block)
		// For now, we'll return an error to indicate waiting
		return fmt.Errorf("would block waiting for lock")
	default:
		return fmt.Errorf("invalid lock mode")
	}
}

// Unlock releases a lock on a resource
func (lm *LockManager) Unlock(txnID TransactionID, resourceID string) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	lock, exists := lm.locks[resourceID]
	if !exists {
		return nil // No lock to release
	}
	
	// For shared locks, we need to handle multiple holders
	// In this simplified implementation, we'll just remove the transaction
	// from waiters and check if it was the holder
	
	// Remove transaction from waiters list for this resource
	for i, waiter := range lock.Waiters {
		if waiter == txnID {
			lock.Waiters = append(lock.Waiters[:i], lock.Waiters[i+1:]...)
			break
		}
	}
	
	// Remove resource from transaction's waiters list
	if waiters, exists := lm.waiters[txnID]; exists {
		for i, waiter := range waiters {
			if waiter == resourceID {
				lm.waiters[txnID] = append(waiters[:i], waiters[i+1:]...)
				break
			}
		}
	}
	
	// If this transaction holds the lock, release it
	if lock.Holder == txnID {
		// If there are waiters, grant lock to next waiter
		if len(lock.Waiters) > 0 {
			nextWaiter := lock.Waiters[0]
			lock.Waiters = lock.Waiters[1:]
			lock.Holder = nextWaiter
			// In a real implementation, we would notify the waiting transaction
		} else {
			// No waiters, remove the lock
			delete(lm.locks, resourceID)
		}
	}
	
	return nil
}

// isCompatible checks if a lock request is compatible with existing locks
func (lm *LockManager) isCompatible(existing *Lock, requested LockType) bool {
	// Shared locks are compatible with other shared locks
	// Exclusive locks are not compatible with any other locks
	switch existing.LockType {
	case LockNone:
		return true
	case LockShared:
		return requested == LockShared
	case LockExclusive:
		return false
	default:
		return false
	}
}

// GetLockInfo returns information about locks on a resource
func (lm *LockManager) GetLockInfo(resourceID string) (*Lock, bool) {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	lock, exists := lm.locks[resourceID]
	return lock, exists
}

// GetWaitingTransactions returns transactions waiting for a resource
func (lm *LockManager) GetWaitingTransactions(resourceID string) []TransactionID {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	
	lock, exists := lm.locks[resourceID]
	if !exists {
		return nil
	}
	
	// Return a copy of the waiters slice
	waiters := make([]TransactionID, len(lock.Waiters))
	copy(waiters, lock.Waiters)
	return waiters
}

// detectDeadlock checks for deadlocks using wait-for graph
func (dd *DeadlockDetector) detectDeadlock(requester TransactionID, resourceID string) bool {
	// Simple deadlock detection: check if there's a cycle in the wait-for graph
	visited := make(map[TransactionID]bool)
	return dd.hasCycle(requester, visited)
}

// hasCycle detects cycles in the wait-for graph using DFS
func (dd *DeadlockDetector) hasCycle(txnID TransactionID, visited map[TransactionID]bool) bool {
	if visited[txnID] {
		return true // Cycle detected
	}
	
	visited[txnID] = true
	defer delete(visited, txnID)
	
	// Get resources this transaction is waiting for
	dd.lockManager.mu.RLock()
	waitingFor, exists := dd.lockManager.waiters[txnID]
	dd.lockManager.mu.RUnlock()
	
	if !exists {
		return false
	}
	
	// Check each resource
	for _, resourceID := range waitingFor {
		dd.lockManager.mu.RLock()
		lock, exists := dd.lockManager.locks[resourceID]
		dd.lockManager.mu.RUnlock()
		
		if !exists {
			continue
		}
		
		// Check if the holder of this lock is waiting for other resources
		if dd.hasCycle(lock.Holder, visited) {
			return true
		}
	}
	
	return false
}

// ForceUnlock forcefully removes all locks held by a transaction
func (lm *LockManager) ForceUnlock(txnID TransactionID) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	
	// Remove from all locks
	for resourceID, lock := range lm.locks {
		if lock.Holder == txnID {
			// Grant lock to next waiter if available
			if len(lock.Waiters) > 0 {
				nextWaiter := lock.Waiters[0]
				lock.Waiters = lock.Waiters[1:]
				lock.Holder = nextWaiter
			} else {
				// No waiters, remove the lock
				delete(lm.locks, resourceID)
			}
		} else {
			// Remove from waiters list
			for i, waiter := range lock.Waiters {
				if waiter == txnID {
					lock.Waiters = append(lock.Waiters[:i], lock.Waiters[i+1:]...)
					break
				}
			}
		}
	}
	
	// Remove from waiters map
	delete(lm.waiters, txnID)
}

// GetLockCount returns the total number of active locks
func (lm *LockManager) GetLockCount() int {
	lm.mu.RLock()
	defer lm.mu.RUnlock()
	return len(lm.locks)
}