package utils

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// DeadlockDetector detects and prevents deadlocks in transactions
// Optimized version that reduces synchronization overhead by using
// efficient data structures and minimizing mutex operations
type DeadlockDetector struct {
	mu            sync.Mutex
	activeTxns    map[uint64]*TransactionInfo
	waitGraph     map[uint64]map[uint64]bool
	keyHolders    map[string]uint64 // Maps keys to the transaction ID that holds them
	
	detectionInterval time.Duration
	stopChan      chan struct{}
	wg            sync.WaitGroup
	abortCallback func(uint64) // Callback to abort a transaction
}

// TransactionInfo holds information about an active transaction
type TransactionInfo struct {
	txnID       uint64
	startTime   time.Time
	locksHeld   map[string]bool
	locksWaiting map[string]bool
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector(detectionInterval time.Duration, abortCallback func(uint64)) *DeadlockDetector {
	dd := &DeadlockDetector{
		activeTxns:    make(map[uint64]*TransactionInfo),
		waitGraph:     make(map[uint64]map[uint64]bool),
		keyHolders:    make(map[string]uint64),
		detectionInterval: detectionInterval,
		stopChan:      make(chan struct{}),
		abortCallback: abortCallback,
	}
	
	dd.wg.Add(1)
	go dd.runDetection()
	
	return dd
}

// AddTransaction adds a transaction to the deadlock detector
func (dd *DeadlockDetector) AddTransaction(txnID uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	txnInfo := &TransactionInfo{
		txnID:        txnID,
		startTime:    time.Now(),
		locksHeld:    make(map[string]bool),
		locksWaiting: make(map[string]bool),
	}
	
	dd.activeTxns[txnID] = txnInfo
	dd.waitGraph[txnID] = make(map[uint64]bool)
}

// RemoveTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	txnInfo, exists := dd.activeTxns[txnID]
	if !exists {
		return
	}
	
	// Remove this transaction from keyHolders
	for key := range txnInfo.locksHeld {
		if holder, ok := dd.keyHolders[key]; ok && holder == txnID {
			delete(dd.keyHolders, key)
		}
	}
	
	// Remove this transaction from all other transactions' wait lists
	for _, waitMap := range dd.waitGraph {
		delete(waitMap, txnID)
	}
	
	// Remove from our tracking
	delete(dd.activeTxns, txnID)
	delete(dd.waitGraph, txnID)
}

// AddLock adds a lock held by a transaction
func (dd *DeadlockDetector) AddLock(txnID uint64, key string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	if txnInfo, exists := dd.activeTxns[txnID]; exists {
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key)
		dd.keyHolders[key] = txnID
	}
}

// AddWaitingLock adds a lock that a transaction is waiting for
func (dd *DeadlockDetector) AddWaitingLock(txnID uint64, key string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	if txnInfo, exists := dd.activeTxns[txnID]; exists {
		txnInfo.locksWaiting[key] = true
	}
}

// RemoveWaitingLock removes a waiting lock from a transaction
func (dd *DeadlockDetector) RemoveWaitingLock(txnID uint64, key string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	if txnInfo, exists := dd.activeTxns[txnID]; exists {
		delete(txnInfo.locksWaiting, key)
	}
}

// CheckForConflicts checks for conflicts with the given key and updates wait graph
// Optimized to use keyHolders map for O(1) lookup instead of O(N) scanning
func (dd *DeadlockDetector) CheckForConflicts(currentTxnID uint64, key string) error {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	// Check if any transaction holds this key
	if conflictTxnID, exists := dd.keyHolders[key]; exists && conflictTxnID != currentTxnID {
		// Conflict found, add to wait graph
		// Ensure the waitGraph entry exists for currentTxnID
		if _, exists := dd.waitGraph[currentTxnID]; !exists {
			dd.waitGraph[currentTxnID] = make(map[uint64]bool)
		}
		dd.waitGraph[currentTxnID][conflictTxnID] = true
		
		// Mark that current transaction is waiting for this key
		if txnInfo, exists := dd.activeTxns[currentTxnID]; exists {
			txnInfo.locksWaiting[key] = true
		}
		
		// Check for deadlock
		if dd.hasCycle(currentTxnID) {
			// Deadlock detected, abort the younger transaction
			dd.abortYoungestTransactionLocked(currentTxnID, conflictTxnID)
			return storage.ErrConflict
		}
		
		return storage.ErrConflict
	}
	
	// No conflict, mark that this transaction now holds this lock
	if txnInfo, exists := dd.activeTxns[currentTxnID]; exists {
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key) // Remove from waiting if it was waiting
		dd.keyHolders[key] = currentTxnID
		
		// Ensure the waitGraph entry exists for currentTxnID
		if _, exists := dd.waitGraph[currentTxnID]; !exists {
			dd.waitGraph[currentTxnID] = make(map[uint64]bool)
		}
	}
	
	return nil
}

// hasCycle detects cycles in the wait graph using DFS
func (dd *DeadlockDetector) hasCycle(startTxnID uint64) bool {
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)
	
	return dd.hasCycleUtil(startTxnID, visited, recStack)
}

// hasCycleUtil is a helper function for cycle detection
func (dd *DeadlockDetector) hasCycleUtil(txnID uint64, visited, recStack map[uint64]bool) bool {
	// If not visited, mark as visited and add to recursion stack
	if !visited[txnID] {
		visited[txnID] = true
		recStack[txnID] = true
		
		// Recur for all transactions that this transaction is waiting for
		if waitList, exists := dd.waitGraph[txnID]; exists {
			for waitingTxnID := range waitList {
				if !visited[waitingTxnID] {
					if dd.hasCycleUtil(waitingTxnID, visited, recStack) {
						return true
					}
				} else if recStack[waitingTxnID] {
					return true // Cycle found
				}
			}
		}
		recStack[txnID] = false
		return false
	} else if recStack[txnID] {
		// If already visited and in recursion stack, we found a cycle
		return true
	}
	
	return false
}

// runDetection runs the periodic deadlock detection
func (dd *DeadlockDetector) runDetection() {
	defer dd.wg.Done()
	
	ticker := time.NewTicker(dd.detectionInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-ticker.C:
			dd.detectAndResolveDeadlocks()
		case <-dd.stopChan:
			return
		}
	}
}

// detectAndResolveDeadlocks detects and resolves deadlocks
func (dd *DeadlockDetector) detectAndResolveDeadlocks() {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	// Collect active transaction IDs
	txnIDs := make([]uint64, 0, len(dd.activeTxns))
	for txnID := range dd.activeTxns {
		txnIDs = append(txnIDs, txnID)
	}
	
	// Simple deadlock resolution: abort the youngest transaction in each cycle
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)
	
	for _, txnID := range txnIDs {
		if !visited[txnID] {
			dd.detectCycleAndAbort(txnID, visited, recStack)
		}
	}
}

// detectCycleAndAbort detects cycles and aborts transactions to resolve deadlocks
func (dd *DeadlockDetector) detectCycleAndAbort(txnID uint64, visited, recStack map[uint64]bool) {
	if visited[txnID] {
		return
	}
	
	visited[txnID] = true
	recStack[txnID] = true
	
	// Check transactions that this transaction is waiting for
	if waitList, exists := dd.waitGraph[txnID]; exists {
		for waitingTxnID := range waitList {
			if !visited[waitingTxnID] {
				dd.detectCycleAndAbort(waitingTxnID, visited, recStack)
			} else if recStack[waitingTxnID] {
				// Found a cycle, abort the youngest transaction
				dd.abortYoungestTransactionLocked(txnID, waitingTxnID)
				// After aborting, we should stop processing this branch
				recStack[txnID] = false
				return
			}
		}
	}
	
	recStack[txnID] = false
}

// abortYoungestTransaction aborts the youngest transaction in a deadlock cycle
func (dd *DeadlockDetector) abortYoungestTransaction(txnID1, txnID2 uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	dd.abortYoungestTransactionLocked(txnID1, txnID2)
}

// abortYoungestTransactionLocked aborts the youngest transaction in a deadlock cycle
// This function assumes the mutex is already locked
func (dd *DeadlockDetector) abortYoungestTransactionLocked(txnID1, txnID2 uint64) {
	// Get transaction info for both transactions
	txnInfo1, exists1 := dd.activeTxns[txnID1]
	txnInfo2, exists2 := dd.activeTxns[txnID2]
	
	if !exists1 || !exists2 {
		return
	}
	
	// Determine which transaction is younger
	var abortTxnID uint64
	if txnInfo1.startTime.After(txnInfo2.startTime) {
		abortTxnID = txnID1
	} else {
		abortTxnID = txnID2
	}
	
	// Remove from our tracking
	dd.removeTransactionLocked(abortTxnID)
	
	// Call the abort callback if provided
	if dd.abortCallback != nil {
		dd.abortCallback(abortTxnID)
	}
}

// removeTransactionLocked removes a transaction from the deadlock detector
// This function assumes the mutex is already locked
func (dd *DeadlockDetector) removeTransactionLocked(txnID uint64) {
	txnInfo, exists := dd.activeTxns[txnID]
	if !exists {
		return
	}
	
	// Remove this transaction from keyHolders
	for key := range txnInfo.locksHeld {
		if holder, ok := dd.keyHolders[key]; ok && holder == txnID {
			delete(dd.keyHolders, key)
		}
	}
	
	// Remove this transaction from all other transactions' wait lists
	for _, waitMap := range dd.waitGraph {
		delete(waitMap, txnID)
	}
	
	// Remove from our tracking
	delete(dd.activeTxns, txnID)
	delete(dd.waitGraph, txnID)
}

// Close stops the deadlock detector
func (dd *DeadlockDetector) Close() {
	// Check if already closed to prevent panic
	select {
	case <-dd.stopChan:
		// Already closed, return immediately
		return
	default:
		// Not closed yet, proceed with closing
		close(dd.stopChan)
		dd.wg.Wait()
	}
}