package utils

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// DeadlockDetector detects and prevents deadlocks in transactions
type DeadlockDetector struct {
	mu            sync.RWMutex
	waitGraph     map[uint64]map[uint64]bool // txnID -> {waitingForTxnID -> true}
	activeTxns    map[uint64]*TransactionInfo
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
		waitGraph:     make(map[uint64]map[uint64]bool),
		activeTxns:    make(map[uint64]*TransactionInfo),
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
	
	dd.activeTxns[txnID] = &TransactionInfo{
		txnID:        txnID,
		startTime:    time.Now(),
		locksHeld:    make(map[string]bool),
		locksWaiting: make(map[string]bool),
	}
	
	if _, exists := dd.waitGraph[txnID]; !exists {
		dd.waitGraph[txnID] = make(map[uint64]bool)
	}
}

// RemoveTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	delete(dd.activeTxns, txnID)
	delete(dd.waitGraph, txnID)
	
	// Remove this transaction from all other transactions' wait lists
	for _, waits := range dd.waitGraph {
		delete(waits, txnID)
	}
}

// AddLock adds a lock held by a transaction
func (dd *DeadlockDetector) AddLock(txnID uint64, key string) {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	if txnInfo, exists := dd.activeTxns[txnID]; exists {
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key)
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
func (dd *DeadlockDetector) CheckForConflicts(currentTxnID uint64, key string) error {
	dd.mu.Lock()
	defer dd.mu.Unlock()
	
	// Check if any other active transaction has written to this key
	conflictFound := false
	var conflictingTxnID uint64
	
	for txnID, txnInfo := range dd.activeTxns {
		// Skip the current transaction
		if txnID == currentTxnID {
			continue
		}
		
		// Check if the other transaction has written to this key
		if _, written := txnInfo.locksHeld[key]; written {
			conflictFound = true
			conflictingTxnID = txnID
			break
		}
	}
	
	if conflictFound {
		// Add to wait graph - currentTxnID is waiting for conflictingTxnID
		if waits, exists := dd.waitGraph[currentTxnID]; exists {
			waits[conflictingTxnID] = true
		} else {
			dd.waitGraph[currentTxnID] = map[uint64]bool{conflictingTxnID: true}
		}
		
		// Mark that current transaction is waiting for this key
		if txnInfo, exists := dd.activeTxns[currentTxnID]; exists {
			txnInfo.locksWaiting[key] = true
		}
		
		// Check for deadlock
		if dd.hasCycle(currentTxnID) {
			// Deadlock detected, abort the younger transaction
			dd.abortYoungestTransaction(currentTxnID, conflictingTxnID)
			return storage.ErrConflict
		}
		
		return storage.ErrConflict
	}
	
	// No conflict, mark that this transaction now holds this lock
	if txnInfo, exists := dd.activeTxns[currentTxnID]; exists {
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key) // Remove from waiting if it was waiting
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
	if !visited[txnID] {
		visited[txnID] = true
		recStack[txnID] = true
		
		// Recur for all transactions that this transaction is waiting for
		if waits, exists := dd.waitGraph[txnID]; exists {
			for waitingTxnID := range waits {
				if !visited[waitingTxnID] && dd.hasCycleUtil(waitingTxnID, visited, recStack) {
					return true
				} else if recStack[waitingTxnID] {
					return true
				}
			}
		}
	}
	
	recStack[txnID] = false
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
	
	// Simple deadlock resolution: abort the youngest transaction in each cycle
	visited := make(map[uint64]bool)
	recStack := make(map[uint64]bool)
	
	for txnID := range dd.activeTxns {
		if !visited[txnID] {
			dd.detectCycleAndAbort(txnID, visited, recStack)
		}
	}
}

// detectCycleAndAbort detects cycles and aborts transactions to resolve deadlocks
func (dd *DeadlockDetector) detectCycleAndAbort(txnID uint64, visited, recStack map[uint64]bool) {
	visited[txnID] = true
	recStack[txnID] = true
	
	// Check transactions that this transaction is waiting for
	if waits, exists := dd.waitGraph[txnID]; exists {
		for waitingTxnID := range waits {
			if !visited[waitingTxnID] {
				dd.detectCycleAndAbort(waitingTxnID, visited, recStack)
			} else if recStack[waitingTxnID] {
				// Found a cycle, abort the youngest transaction
				dd.abortYoungestTransaction(txnID, waitingTxnID)
			}
		}
	}
	
	recStack[txnID] = false
}

// abortYoungestTransaction aborts the youngest transaction in a deadlock cycle
func (dd *DeadlockDetector) abortYoungestTransaction(txnID1, txnID2 uint64) {
	// Get transaction info for both transactions
	txnInfo1, exists1 := dd.activeTxns[txnID1]
	txnInfo2, exists2 := dd.activeTxns[txnID2]
	
	if !exists1 || !exists2 {
		return
	}
	
	// Abort the transaction with the later start time (younger transaction)
	var abortTxnID uint64
	if txnInfo1.startTime.After(txnInfo2.startTime) {
		abortTxnID = txnID1
	} else {
		abortTxnID = txnID2
	}
	
	// Remove from our tracking
	delete(dd.activeTxns, abortTxnID)
	delete(dd.waitGraph, abortTxnID)
	
	// Remove this transaction from all other transactions' wait lists
	for _, waits := range dd.waitGraph {
		delete(waits, abortTxnID)
	}
	
	// Call the abort callback if provided
	if dd.abortCallback != nil {
		dd.abortCallback(abortTxnID)
	}
}

// Close stops the deadlock detector
func (dd *DeadlockDetector) Close() {
	close(dd.stopChan)
	dd.wg.Wait()
}