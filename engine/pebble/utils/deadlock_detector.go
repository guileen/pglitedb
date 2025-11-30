package utils

import (
	"sync"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// DeadlockDetector detects and prevents deadlocks in transactions
// Optimized version that reduces synchronization overhead by using
// lock-free data structures and minimizing mutex operations
type DeadlockDetector struct {
	// Use sync.Map for lock-free access to active transactions
	activeTxns    sync.Map // uint64 -> *TransactionInfo
	waitGraph     sync.Map // uint64 -> *sync.Map (uint64 -> bool)
	
	detectionInterval time.Duration
	stopChan      chan struct{}
	wg            sync.WaitGroup
	abortCallback func(uint64) // Callback to abort a transaction
}

// TransactionInfo holds information about an active transaction
// Optimized to reduce mutex contention by using atomic operations where possible
type TransactionInfo struct {
	txnID       uint64
	startTime   time.Time
	locksHeld   sync.Map // string -> bool
	locksWaiting sync.Map // string -> bool
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector(detectionInterval time.Duration, abortCallback func(uint64)) *DeadlockDetector {
	dd := &DeadlockDetector{
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
	}
	
	dd.activeTxns.Store(txnID, txnInfo)
	
	// Initialize wait graph entry
	dd.waitGraph.Store(txnID, &sync.Map{})
}

// RemoveTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	dd.activeTxns.Delete(txnID)
	dd.waitGraph.Delete(txnID)
	
	// Remove this transaction from all other transactions' wait lists
	dd.waitGraph.Range(func(key, value interface{}) bool {
		waitMap := value.(*sync.Map)
		waitMap.Delete(txnID)
		return true
	})
}

// AddLock adds a lock held by a transaction
func (dd *DeadlockDetector) AddLock(txnID uint64, key string) {
	if txnInfo, ok := dd.activeTxns.Load(txnID); ok {
		info := txnInfo.(*TransactionInfo)
		info.locksHeld.Store(key, true)
		info.locksWaiting.Delete(key)
	}
}

// AddWaitingLock adds a lock that a transaction is waiting for
func (dd *DeadlockDetector) AddWaitingLock(txnID uint64, key string) {
	if txnInfo, ok := dd.activeTxns.Load(txnID); ok {
		info := txnInfo.(*TransactionInfo)
		info.locksWaiting.Store(key, true)
	}
}

// RemoveWaitingLock removes a waiting lock from a transaction
func (dd *DeadlockDetector) RemoveWaitingLock(txnID uint64, key string) {
	if txnInfo, ok := dd.activeTxns.Load(txnID); ok {
		info := txnInfo.(*TransactionInfo)
		info.locksWaiting.Delete(key)
	}
}

// CheckForConflicts checks for conflicts with the given key and updates wait graph
// Optimized to minimize mutex operations and reduce lock contention
func (dd *DeadlockDetector) CheckForConflicts(currentTxnID uint64, key string) error {
	var conflictTxnID uint64
	var conflictFound bool
	
	// Check for conflicts with each active transaction
	dd.activeTxns.Range(func(txnKey, txnValue interface{}) bool {
		txnID := txnKey.(uint64)
		
		// Skip the current transaction
		if txnID == currentTxnID {
			return true
		}
		
		txnInfo := txnValue.(*TransactionInfo)
		
		// Check if this transaction has written to the key
		if _, written := txnInfo.locksHeld.Load(key); written {
			conflictFound = true
			conflictTxnID = txnID
			return false // Stop iteration
		}
		
		return true
	})
	
	if conflictFound {
		// Add to wait graph - currentTxnID is waiting for conflictingTxnID
		if waitMapInterface, ok := dd.waitGraph.Load(currentTxnID); ok {
			waitMap := waitMapInterface.(*sync.Map)
			waitMap.Store(conflictTxnID, true)
		}
		
		// Mark that current transaction is waiting for this key
		if txnInfo, ok := dd.activeTxns.Load(currentTxnID); ok {
			info := txnInfo.(*TransactionInfo)
			info.locksWaiting.Store(key, true)
		}
		
		// Check for deadlock
		if dd.hasCycle(currentTxnID) {
			// Deadlock detected, abort the younger transaction
			dd.abortYoungestTransaction(currentTxnID, conflictTxnID)
			return storage.ErrConflict
		}
		
		return storage.ErrConflict
	}
	
	// No conflict, mark that this transaction now holds this lock
	if txnInfo, ok := dd.activeTxns.Load(currentTxnID); ok {
		info := txnInfo.(*TransactionInfo)
		info.locksHeld.Store(key, true)
		info.locksWaiting.Delete(key) // Remove from waiting if it was waiting
	}
	
	return nil
}

// hasCycle detects cycles in the wait graph using DFS
// Uses atomic operations to minimize lock contention
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
		if waitMapInterface, ok := dd.waitGraph.Load(txnID); ok {
			waitMap := waitMapInterface.(*sync.Map)
			waitMap.Range(func(key, value interface{}) bool {
				waitingTxnID := key.(uint64)
				if !visited[waitingTxnID] && dd.hasCycleUtil(waitingTxnID, visited, recStack) {
					return false // Stop iteration
				} else if recStack[waitingTxnID] {
					return false // Stop iteration (cycle found)
				}
				return true
			})
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
	// Collect active transaction IDs
	var txnIDs []uint64
	dd.activeTxns.Range(func(key, value interface{}) bool {
		txnIDs = append(txnIDs, key.(uint64))
		return true
	})
	
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
	if waitMapInterface, ok := dd.waitGraph.Load(txnID); ok {
		waitMap := waitMapInterface.(*sync.Map)
		waitMap.Range(func(key, value interface{}) bool {
			waitingTxnID := key.(uint64)
			if !visited[waitingTxnID] {
				dd.detectCycleAndAbort(waitingTxnID, visited, recStack)
			} else if recStack[waitingTxnID] {
				// Found a cycle, abort the youngest transaction
				dd.abortYoungestTransaction(txnID, waitingTxnID)
			}
			return true
		})
	}
	
	recStack[txnID] = false
}

// abortYoungestTransaction aborts the youngest transaction in a deadlock cycle
func (dd *DeadlockDetector) abortYoungestTransaction(txnID1, txnID2 uint64) {
	// Get transaction info for both transactions
	txnInfo1Interface, exists1 := dd.activeTxns.Load(txnID1)
	txnInfo2Interface, exists2 := dd.activeTxns.Load(txnID2)
	
	if !exists1 || !exists2 {
		return
	}
	
	txnInfo1 := txnInfo1Interface.(*TransactionInfo)
	txnInfo2 := txnInfo2Interface.(*TransactionInfo)
	
	// Determine which transaction is younger
	var abortTxnID uint64
	if txnInfo1.startTime.After(txnInfo2.startTime) {
		abortTxnID = txnID1
	} else {
		abortTxnID = txnID2
	}
	
	// Remove from our tracking
	dd.activeTxns.Delete(abortTxnID)
	dd.waitGraph.Delete(abortTxnID)
	
	// Remove this transaction from all other transactions' wait lists
	dd.waitGraph.Range(func(key, value interface{}) bool {
		waitMap := value.(*sync.Map)
		waitMap.Delete(abortTxnID)
		return true
	})
	
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