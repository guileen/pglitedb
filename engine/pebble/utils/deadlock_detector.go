package utils

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// DeadlockDetector detects and prevents deadlocks in transactions
type DeadlockDetector struct {
	// Use a single mutex for the shared data structures
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
	mu          sync.RWMutex // Mutex to protect the maps
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
	txnInfo := &TransactionInfo{
		txnID:        txnID,
		startTime:    time.Now(),
		locksHeld:    make(map[string]bool),
		locksWaiting: make(map[string]bool),
	}
	
	dd.mu.Lock()
	dd.activeTxns[txnID] = txnInfo
	if _, exists := dd.waitGraph[txnID]; !exists {
		dd.waitGraph[txnID] = make(map[uint64]bool)
	}
	dd.mu.Unlock()
}

// RemoveTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	dd.mu.Lock()
	delete(dd.activeTxns, txnID)
	delete(dd.waitGraph, txnID)
	
	// Remove this transaction from all other transactions' wait lists
	for _, waits := range dd.waitGraph {
		delete(waits, txnID)
	}
	dd.mu.Unlock()
}

// AddLock adds a lock held by a transaction
func (dd *DeadlockDetector) AddLock(txnID uint64, key string) {
	dd.mu.RLock()
	txnInfo, exists := dd.activeTxns[txnID]
	dd.mu.RUnlock()
	
	if exists {
		txnInfo.mu.Lock()
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key)
		txnInfo.mu.Unlock()
	}
}

// AddWaitingLock adds a lock that a transaction is waiting for
func (dd *DeadlockDetector) AddWaitingLock(txnID uint64, key string) {
	dd.mu.RLock()
	txnInfo, exists := dd.activeTxns[txnID]
	dd.mu.RUnlock()
	
	if exists {
		txnInfo.mu.Lock()
		txnInfo.locksWaiting[key] = true
		txnInfo.mu.Unlock()
	}
}

// RemoveWaitingLock removes a waiting lock from a transaction
func (dd *DeadlockDetector) RemoveWaitingLock(txnID uint64, key string) {
	dd.mu.RLock()
	txnInfo, exists := dd.activeTxns[txnID]
	dd.mu.RUnlock()
	
	if exists {
		txnInfo.mu.Lock()
		delete(txnInfo.locksWaiting, key)
		txnInfo.mu.Unlock()
	}
}

// CheckForConflicts checks for conflicts with the given key and updates wait graph
func (dd *DeadlockDetector) CheckForConflicts(currentTxnID uint64, key string) error {
	// Get a copy of active transaction IDs to minimize lock time
	dd.mu.RLock()
	txnIDs := make([]uint64, 0, len(dd.activeTxns))
	for txnID := range dd.activeTxns {
		txnIDs = append(txnIDs, txnID)
	}
	dd.mu.RUnlock()
	
	// Check for conflicts with each active transaction
	conflictFound := false
	var conflictingTxnID uint64
	
	for _, txnID := range txnIDs {
		// Skip the current transaction
		if txnID == currentTxnID {
			continue
		}
		
		// Check if this transaction has written to the key
		dd.mu.RLock()
		txnInfo, exists := dd.activeTxns[txnID]
		dd.mu.RUnlock()
		
		if exists {
			txnInfo.mu.RLock()
			_, written := txnInfo.locksHeld[key]
			txnInfo.mu.RUnlock()
			
			if written {
				conflictFound = true
				conflictingTxnID = txnID
				break
			}
		}
	}
	
	if conflictFound {
		// Add to wait graph - currentTxnID is waiting for conflictingTxnID
		dd.mu.Lock()
		if waits, exists := dd.waitGraph[currentTxnID]; exists {
			waits[conflictingTxnID] = true
		} else {
			dd.waitGraph[currentTxnID] = map[uint64]bool{conflictingTxnID: true}
		}
		dd.mu.Unlock()
		
		// Mark that current transaction is waiting for this key
		dd.mu.RLock()
		currentTxnInfo, exists := dd.activeTxns[currentTxnID]
		dd.mu.RUnlock()
		
		if exists {
			currentTxnInfo.mu.Lock()
			currentTxnInfo.locksWaiting[key] = true
			currentTxnInfo.mu.Unlock()
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
	dd.mu.RLock()
	currentTxnInfo, exists := dd.activeTxns[currentTxnID]
	dd.mu.RUnlock()
	
	if exists {
		currentTxnInfo.mu.Lock()
		currentTxnInfo.locksHeld[key] = true
		delete(currentTxnInfo.locksWaiting, key) // Remove from waiting if it was waiting
		currentTxnInfo.mu.Unlock()
	}
	
	return nil
}

// hasCycle detects cycles in the wait graph using DFS
func (dd *DeadlockDetector) hasCycle(startTxnID uint64) bool {
	dd.mu.RLock()
	defer dd.mu.RUnlock()
	
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
		// Note: mu is already locked by hasCycle
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
	// Get a copy of active transaction IDs to minimize lock time
	dd.mu.RLock()
	txnIDs := make([]uint64, 0, len(dd.activeTxns))
	for txnID := range dd.activeTxns {
		txnIDs = append(txnIDs, txnID)
	}
	dd.mu.RUnlock()
	
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
	visited[txnID] = true
	recStack[txnID] = true
	
	// Check transactions that this transaction is waiting for
	dd.mu.RLock()
	waits, exists := dd.waitGraph[txnID]
	dd.mu.RUnlock()
	
	if exists {
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
	dd.mu.RLock()
	txnInfo1, exists1 := dd.activeTxns[txnID1]
	txnInfo2, exists2 := dd.activeTxns[txnID2]
	dd.mu.RUnlock()
	
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
	dd.mu.Lock()
	delete(dd.activeTxns, abortTxnID)
	delete(dd.waitGraph, abortTxnID)
	
	// Remove this transaction from all other transactions' wait lists
	for _, waits := range dd.waitGraph {
		delete(waits, abortTxnID)
	}
	dd.mu.Unlock()
	
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