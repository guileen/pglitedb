package utils

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/guileen/pglitedb/storage"
)

// DeadlockDetector detects and prevents deadlocks in transactions
// Optimized version that reduces synchronization overhead by using
// efficient data structures and minimizing mutex operations
type DeadlockDetector struct {
	activeTxns sync.Map // map[uint64]*TransactionInfo
	waitGraph  sync.Map // map[uint64]map[uint64]bool
	keyHolders sync.Map // map[string]uint64 // Maps keys to the transaction ID that holds them

	detectionInterval time.Duration
	stopChan          chan struct{}
	wg                sync.WaitGroup
	abortCallback     func(uint64) // Callback to abort a transaction

	// Atomic counters for performance monitoring
	transactionCount int64
	detectionRuns    int64
}

// TransactionInfo holds information about an active transaction
type TransactionInfo struct {
	txnID        uint64
	startTime    time.Time
	locksHeld    map[string]bool
	locksWaiting map[string]bool
}

// NewDeadlockDetector creates a new deadlock detector
func NewDeadlockDetector(detectionInterval time.Duration, abortCallback func(uint64)) *DeadlockDetector {
	dd := &DeadlockDetector{
		detectionInterval: detectionInterval,
		stopChan:          make(chan struct{}),
		abortCallback:     abortCallback,
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

	dd.activeTxns.Store(txnID, txnInfo)
	dd.waitGraph.Store(txnID, make(map[uint64]bool))
	atomic.AddInt64(&dd.transactionCount, 1)
}

// RemoveTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) RemoveTransaction(txnID uint64) {
	txnVal, loaded := dd.activeTxns.LoadAndDelete(txnID)
	if !loaded {
		return
	}

	txnInfo := txnVal.(*TransactionInfo)

	// Remove this transaction from keyHolders
	for key := range txnInfo.locksHeld {
		dd.keyHolders.Delete(key)
	}

	// Remove this transaction from all other transactions' wait lists
	// Collect keys first to avoid concurrent map iteration and map write
	var waitGraphKeys []uint64
	dd.waitGraph.Range(func(key, value interface{}) bool {
		if k, ok := key.(uint64); ok {
			waitGraphKeys = append(waitGraphKeys, k)
		}
		return true
	})

	// Now safely modify the wait lists
	for _, key := range waitGraphKeys {
		if waitVal, loaded := dd.waitGraph.Load(key); loaded {
			if waitList, ok := waitVal.(map[uint64]bool); ok {
				// Create a copy, modify it, and store it back to avoid concurrent map writes
				newWaitList := make(map[uint64]bool)
				for k, v := range waitList {
					newWaitList[k] = v
				}
				delete(newWaitList, txnID)
				dd.waitGraph.Store(key, newWaitList)
			}
		}
	}

	// Remove from waitGraph
	dd.waitGraph.Delete(txnID)

	atomic.AddInt64(&dd.transactionCount, -1)
}

// AddLock adds a lock held by a transaction
func (dd *DeadlockDetector) AddLock(txnID uint64, key string) {
	if txnVal, ok := dd.activeTxns.Load(txnID); ok {
		txnInfo := txnVal.(*TransactionInfo)

		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key)
		dd.keyHolders.Store(key, txnID)
	}
}

// AddWaitingLock adds a lock that a transaction is waiting for
func (dd *DeadlockDetector) AddWaitingLock(txnID uint64, key string) {
	if txnVal, ok := dd.activeTxns.Load(txnID); ok {
		txnInfo := txnVal.(*TransactionInfo)
		txnInfo.locksWaiting[key] = true
	}
}

// RemoveWaitingLock removes a waiting lock from a transaction
func (dd *DeadlockDetector) RemoveWaitingLock(txnID uint64, key string) {
	if txnVal, ok := dd.activeTxns.Load(txnID); ok {
		txnInfo := txnVal.(*TransactionInfo)
		delete(txnInfo.locksWaiting, key)
	}
}

// CheckForConflicts checks for conflicts with the given key and updates wait graph
// Optimized to use keyHolders map for O(1) lookup instead of O(N) scanning
func (dd *DeadlockDetector) CheckForConflicts(currentTxnID uint64, key string) error {
	// Check if any transaction holds this key
	if conflictVal, exists := dd.keyHolders.Load(key); exists {
		if conflictTxnID, ok := conflictVal.(uint64); ok && conflictTxnID != currentTxnID {
			// Conflict found, add to wait graph
			// Ensure the waitGraph entry exists for currentTxnID
			var waitMap map[uint64]bool
			if waitVal, exists := dd.waitGraph.Load(currentTxnID); exists {
				waitMap = waitVal.(map[uint64]bool)
			} else {
				// Need to create a new wait map
				dd.waitGraph.Range(func(key, value interface{}) bool {
					// Double-check if another goroutine already created it
					if k, ok := key.(uint64); ok && k == currentTxnID {
						if wm, ok := value.(map[uint64]bool); ok {
							waitMap = wm
							return false // Stop iteration
						}
					}
					return true
				})

				// If still not found, create new one
				if waitMap == nil {
					waitMap = make(map[uint64]bool)
					dd.waitGraph.Store(currentTxnID, waitMap)
				}
			}

			// Create a copy, modify it, and store it back to avoid concurrent map writes
			newWaitMap := make(map[uint64]bool)
			for k, v := range waitMap {
				newWaitMap[k] = v
			}
			newWaitMap[conflictTxnID] = true
			dd.waitGraph.Store(currentTxnID, newWaitMap)

			// Mark that current transaction is waiting for this key
			if txnVal, exists := dd.activeTxns.Load(currentTxnID); exists {
				txnInfo := txnVal.(*TransactionInfo)
				txnInfo.locksWaiting[key] = true
			}

			// Check for deadlock
			if dd.hasCycle(currentTxnID) {
				// Deadlock detected, abort the younger transaction
				dd.abortYoungestTransaction(currentTxnID, conflictTxnID)
				return storage.ErrConflict
			}

			return storage.ErrConflict
		}
	}

	// No conflict, mark that this transaction now holds this lock
	if txnVal, exists := dd.activeTxns.Load(currentTxnID); exists {
		txnInfo := txnVal.(*TransactionInfo)
		txnInfo.locksHeld[key] = true
		delete(txnInfo.locksWaiting, key) // Remove from waiting if it was waiting
		dd.keyHolders.Store(key, currentTxnID)

		// Ensure the waitGraph entry exists for currentTxnID
		if _, exists := dd.waitGraph.Load(currentTxnID); !exists {
			dd.waitGraph.Store(currentTxnID, make(map[uint64]bool))
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

		// Collect waiting transaction IDs first to avoid concurrent map access
		var waitingTxnIDs []uint64
		if waitVal, exists := dd.waitGraph.Load(txnID); exists {
			if waitList, ok := waitVal.(map[uint64]bool); ok {
				// Create a copy of the waitList to avoid concurrent map iteration and map write
				waitListCopy := make(map[uint64]bool)
				for k, v := range waitList {
					waitListCopy[k] = v
				}

				// Collect all keys from the copy to avoid concurrent map iteration
				for waitingTxnID := range waitListCopy {
					waitingTxnIDs = append(waitingTxnIDs, waitingTxnID)
				}
			}
		}

		// Recur for all transactions that this transaction is waiting for
		for _, waitingTxnID := range waitingTxnIDs {
			if !visited[waitingTxnID] {
				if dd.hasCycleUtil(waitingTxnID, visited, recStack) {
					return true
				}
			} else if recStack[waitingTxnID] {
				return true // Cycle found
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
	atomic.AddInt64(&dd.detectionRuns, 1)

	// Collect active transaction IDs safely
	var txnIDs []uint64
	dd.activeTxns.Range(func(key, value interface{}) bool {
		if txnID, ok := key.(uint64); ok {
			txnIDs = append(txnIDs, txnID)
		}
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
	if visited[txnID] {
		return
	}

	visited[txnID] = true
	recStack[txnID] = true

	// Check transactions that this transaction is waiting for
	// We need to collect the keys first to avoid concurrent map iteration and map write
	var waitingTxnIDs []uint64
	if waitVal, exists := dd.waitGraph.Load(txnID); exists {
		if waitList, ok := waitVal.(map[uint64]bool); ok {
			// Create a copy of the waitList to avoid concurrent map iteration and map write
			waitListCopy := make(map[uint64]bool)
			for k, v := range waitList {
				waitListCopy[k] = v
			}

			// Collect all keys from the copy to avoid concurrent map iteration
			for waitingTxnID := range waitListCopy {
				waitingTxnIDs = append(waitingTxnIDs, waitingTxnID)
			}
		}
	}

	// Now iterate over the collected keys
	for _, waitingTxnID := range waitingTxnIDs {
		if !visited[waitingTxnID] {
			dd.detectCycleAndAbort(waitingTxnID, visited, recStack)
		} else if recStack[waitingTxnID] {
			// Found a cycle, abort the youngest transaction
			dd.abortYoungestTransaction(txnID, waitingTxnID)
			// After aborting, we should stop processing this branch
			recStack[txnID] = false
			return
		}
	}

	recStack[txnID] = false
}

// abortYoungestTransaction aborts the youngest transaction in a deadlock cycle
func (dd *DeadlockDetector) abortYoungestTransaction(txnID1, txnID2 uint64) {
	// Get transaction info for both transactions
	txnVal1, exists1 := dd.activeTxns.Load(txnID1)
	txnVal2, exists2 := dd.activeTxns.Load(txnID2)

	if !exists1 || !exists2 {
		return
	}

	txnInfo1 := txnVal1.(*TransactionInfo)
	txnInfo2 := txnVal2.(*TransactionInfo)

	// Determine which transaction is younger
	var abortTxnID uint64
	if txnInfo1.startTime.After(txnInfo2.startTime) {
		abortTxnID = txnID1
	} else {
		abortTxnID = txnID2
	}

	// Remove from our tracking
	dd.removeTransaction(abortTxnID)

	// Call the abort callback if provided
	if dd.abortCallback != nil {
		dd.abortCallback(abortTxnID)
	}
}

// removeTransaction removes a transaction from the deadlock detector
func (dd *DeadlockDetector) removeTransaction(txnID uint64) {
	txnVal, loaded := dd.activeTxns.LoadAndDelete(txnID)
	if !loaded {
		return
	}

	txnInfo := txnVal.(*TransactionInfo)

	// Remove this transaction from keyHolders
	for key := range txnInfo.locksHeld {
		dd.keyHolders.Delete(key)
	}

	// Remove this transaction from all other transactions' wait lists
	// Collect keys first to avoid concurrent map iteration and map write
	var waitGraphKeys []uint64
	dd.waitGraph.Range(func(key, value interface{}) bool {
		if k, ok := key.(uint64); ok {
			waitGraphKeys = append(waitGraphKeys, k)
		}
		return true
	})

	// Now safely modify the wait lists
	for _, key := range waitGraphKeys {
		if waitVal, loaded := dd.waitGraph.Load(key); loaded {
			if waitList, ok := waitVal.(map[uint64]bool); ok {
				// Create a copy, modify it, and store it back to avoid concurrent map writes
				newWaitList := make(map[uint64]bool)
				for k, v := range waitList {
					newWaitList[k] = v
				}
				delete(newWaitList, txnID)
				dd.waitGraph.Store(key, newWaitList)
			}
		}
	}

	// Remove from waitGraph
	dd.waitGraph.Delete(txnID)

	atomic.AddInt64(&dd.transactionCount, -1)
}

// GetTransactionCount returns the current number of active transactions
func (dd *DeadlockDetector) GetTransactionCount() int64 {
	return atomic.LoadInt64(&dd.transactionCount)
}

// GetDetectionRuns returns the number of detection runs performed
func (dd *DeadlockDetector) GetDetectionRuns() int64 {
	return atomic.LoadInt64(&dd.detectionRuns)
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
