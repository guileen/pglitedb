package utils

import (
	"testing"
	"time"
)

func BenchmarkCheckForConflictsOld(b *testing.B) {
	// This would benchmark the old implementation if we still had it
	// For comparison purposes only
	b.Skip("Skipping old implementation benchmark")
}

func BenchmarkCheckForConflictsNew(b *testing.B) {
	dd := NewDeadlockDetector(100*time.Millisecond, func(txnID uint64) {})
	defer dd.Close()

	// Add a large number of transactions
	numTransactions := 10000
	for i := 0; i < numTransactions; i++ {
		dd.AddTransaction(uint64(i))
		dd.AddLock(uint64(i), "key"+string(rune(i%1000))) // Distribute keys
	}

	b.ResetTimer()
	
	// Benchmark checking for conflicts
	for i := 0; i < b.N; i++ {
		txnID := uint64(i % numTransactions)
		key := "key" + string(rune((i+500)%1000))
		dd.CheckForConflicts(txnID, key)
	}
}

func BenchmarkHasCycle(b *testing.B) {
	dd := NewDeadlockDetector(100*time.Millisecond, func(txnID uint64) {})
	defer dd.Close()

	// Create a chain of waiting transactions
	numTransactions := 1000
	for i := 0; i < numTransactions; i++ {
		dd.AddTransaction(uint64(i))
		if i > 0 {
			// Make transaction i wait for transaction i-1
			if waitMap, exists := dd.waitGraph[uint64(i)]; exists {
				waitMap[uint64(i-1)] = true
			}
		}
	}

	b.ResetTimer()
	
	// Benchmark cycle detection
	for i := 0; i < b.N; i++ {
		txnID := uint64(i % numTransactions)
		dd.hasCycle(txnID)
	}
}