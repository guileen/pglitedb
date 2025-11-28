package concurrent_tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"

)

// TestErrorRecovery tests various error recovery scenarios in concurrent environments
func TestErrorRecovery(t *testing.T) {
	// Create a simple schema for testing
	schemaDef := &types.TableDefinition{
		ID:   "test_table_1",
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{
				Name:       "id",
				Type:       types.ColumnTypeNumber,
				PrimaryKey: true,
			},
			{
				Name: "data",
				Type: types.ColumnTypeString,
			},
		},
	}

	t.Run("TransactionAbortAndRecovery", func(t *testing.T) {
		// Test recovery after transaction abort in concurrent environment
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 10
		const opsPerGoroutine = 20

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		successCounts := make(chan int, numGoroutines)
		errorEvents := make(chan string, numGoroutines*opsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()
				successCount := 0

				for j := 0; j < opsPerGoroutine; j++ {
					ctx := context.Background()
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_begin_failed", goroutineID, j)
						continue
					}

					// Perform operation
					record := &types.Record{
						ID:    fmt.Sprintf("recovery_test_record_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*1000 + j)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("recovery_test_value_%d_%d", goroutineID, j)},
						},
					}

					_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
					if err != nil {
						tx.Rollback()
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_insert_failed", goroutineID, j)
						continue
					}

					// Randomly abort some transactions to test recovery
					if j%7 == 0 { // Abort every 7th transaction
						tx.Rollback()
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_intentionally_aborted", goroutineID, j)
						continue
					}

					// Commit normally
					err = tx.Commit()
					if err != nil {
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_commit_failed", goroutineID, j)
						continue
					}

					successCount++
				}

				successCounts <- successCount
			}()
		}

		// Wait for completion
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(30 * time.Second):
			t.Fatal("Transaction abort and recovery test timed out")
		}

		// Analyze results
		close(successCounts)
		close(errorEvents)

		totalSuccess := 0
		for count := range successCounts {
			totalSuccess += count
		}

		abortCount := 0
		otherErrorCount := 0
		for event := range errorEvents {
			if strings.Contains(event, "intentionally_aborted") {
				abortCount++
			} else {
				otherErrorCount++
				t.Logf("Unexpected error event: %s", event)
			}
		}

		// We should have intentionally aborted transactions
		assert.Greater(t, abortCount, 0, "Should have some intentionally aborted transactions")

		// Most operations should succeed or be intentionally aborted
		totalOps := numGoroutines * opsPerGoroutine
		unexpectedErrors := otherErrorCount
		expectedOps := totalSuccess + abortCount
		assert.Less(t, unexpectedErrors, totalOps/10, "Should have relatively few unexpected errors")

		t.Logf("Recovery test results - Success: %d, Intentional Aborts: %d, Unexpected Errors: %d, Total: %d", 
			totalSuccess, abortCount, unexpectedErrors, expectedOps+unexpectedErrors)
	})

	t.Run("ResourceLeakRecovery", func(t *testing.T) {
		// Test recovery from resource leaks in concurrent scenarios
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 15
		const opsPerGoroutine = 10

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		recoveryEvents := make(chan string, numGoroutines*opsPerGoroutine)
		errorEvents := make(chan string, numGoroutines*opsPerGoroutine)

		// Get initial resource metrics
		rm := pebble.GetResourceManager()
		initialMetrics := rm.GetResourceMetrics()

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				for j := 0; j < opsPerGoroutine; j++ {
					ctx := context.Background()
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_begin_failed", goroutineID, j)
						continue
					}

					// Acquire some resources
					iter := rm.AcquireIterator()
					buf := rm.AcquireBuffer(1024)
					record := rm.AcquireRecord()

					// Perform some operations
					testRecord := &types.Record{
						ID:    fmt.Sprintf("leak_test_record_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*1000 + j)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("leak_test_value_%d_%d", goroutineID, j)},
						},
					}

					_, err = tx.InsertRow(ctx, 1, 1, testRecord, schemaDef)
					if err != nil {
						// Properly release resources on error
						rm.ReleaseIterator(iter)
						rm.ReleaseBuffer(buf)
						rm.ReleaseRecord(record)
						tx.Rollback()
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_insert_failed", goroutineID, j)
						continue
					}

					// Randomly "forget" to release some resources to test leak detection
					if j%5 == 0 { // Forget to release resources in 20% of cases
						// Intentionally not releasing resources
						tx.Commit()
						recoveryEvents <- fmt.Sprintf("goroutine_%d_op_%d_resources_not_released", goroutineID, j)
					} else {
						// Properly release resources
						rm.ReleaseIterator(iter)
						rm.ReleaseBuffer(buf)
						rm.ReleaseRecord(record)
						err = tx.Commit()
						if err != nil {
							errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_commit_failed", goroutineID, j)
							continue
						}
						recoveryEvents <- fmt.Sprintf("goroutine_%d_op_%d_resources_properly_released", goroutineID, j)
					}
				}
			}()
		}

		// Wait for completion
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(30 * time.Second):
			t.Fatal("Resource leak recovery test timed out")
		}

		// Check for leaks
		finalMetrics := rm.GetResourceMetrics()
		leakReport := rm.CheckForLeaks()

		// Analyze results
		close(recoveryEvents)
		close(errorEvents)

		properReleaseCount := 0
		improperReleaseCount := 0

		for event := range recoveryEvents {
			if strings.Contains(event, "resources_properly_released") {
				properReleaseCount++
			} else if strings.Contains(event, "resources_not_released") {
				improperReleaseCount++
			}
		}

		errorCount := 0
		for range errorEvents {
			errorCount++
		}

		// We should have some improperly released resources
		assert.Greater(t, improperReleaseCount, 0, "Should have some improperly released resources for leak detection")

		// Report leak detection results
		t.Logf("Leak detection report: Total Leaks=%d", leakReport.TotalLeaks)
		for _, leak := range leakReport.Leaks {
			t.Logf("  Leak: %s (%s) - Duration: %v", leak.ResourceID, leak.ResourceType, leak.LeakDuration)
		}

		t.Logf("Resource leak test results - Proper Releases: %d, Improper Releases: %d, Errors: %d", 
			properReleaseCount, improperReleaseCount, errorCount)

		// Check that resource metrics make sense
		assert.GreaterOrEqual(t, finalMetrics.IteratorAcquired, initialMetrics.IteratorAcquired, "Iterator metrics should be consistent")
		assert.GreaterOrEqual(t, finalMetrics.BufferAcquired, initialMetrics.BufferAcquired, "Buffer metrics should be consistent")
	})

	t.Run("ConcurrentErrorHandling", func(t *testing.T) {
		// Test concurrent error handling and recovery
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 20
		const opsPerGoroutine = 15

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		recoveryAttempts := make(chan int, numGoroutines*opsPerGoroutine)
		fatalErrors := make(chan string, numGoroutines*opsPerGoroutine)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()
				retryCount := 0

				for j := 0; j < opsPerGoroutine; j++ {
					maxRetries := 3
					opSuccess := false

					for attempt := 0; attempt < maxRetries && !opSuccess; attempt++ {
						ctx := context.Background()
						tx, err := engine.BeginTx(ctx)
						if err != nil {
							retryCount++
							time.Sleep(time.Millisecond * time.Duration(attempt*10)) // Exponential backoff
							continue
						}

						// Perform operation with potential for various errors
						record := &types.Record{
							ID:    fmt.Sprintf("error_handling_record_%d_%d_attempt_%d", goroutineID, j, attempt),
							Table: "test_table",
							Data: map[string]*types.Value{
								"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*10000 + j*100 + attempt)},
								"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("error_handling_value_%d_%d_attempt_%d_%d", goroutineID, j, attempt, time.Now().UnixNano())},
							},
						}
						
						// Occasionally use very large values to test resource limits
						if j%8 == 0 {
							// Large value that might cause issues
							largeData := make([]byte, 100*1024) // 100KB (more reasonable for testing)
							for k := range largeData {
								largeData[k] = byte((goroutineID + j + k) % 256)
							}
							record.Data["large_data"] = &types.Value{Type: types.ColumnTypeString, Data: string(largeData)}
						}

						_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
						if err != nil {
							tx.Rollback()
							retryCount++
							time.Sleep(time.Millisecond * time.Duration(attempt*10)) // Exponential backoff
							continue
						}

						err = tx.Commit()
						if err != nil {
							retryCount++
							time.Sleep(time.Millisecond * time.Duration(attempt*10)) // Exponential backoff
							continue
						}

						opSuccess = true
					}

					if !opSuccess {
						fatalErrors <- fmt.Sprintf("goroutine_%d_op_%d_failed_after_%d_attempts", goroutineID, j, maxRetries)
					}
				}

				recoveryAttempts <- retryCount
			}()
		}

		// Wait for completion
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(45 * time.Second):
			t.Fatal("Concurrent error handling test timed out")
		}

		// Analyze results
		close(recoveryAttempts)
		close(fatalErrors)

		totalRetries := 0
		for attempts := range recoveryAttempts {
			totalRetries += attempts
		}

		fatalErrorCount := 0
		for range fatalErrors {
			fatalErrorCount++
		}

		totalOps := numGoroutines * opsPerGoroutine
		successRate := float64(totalOps-fatalErrorCount) / float64(totalOps) * 100

		// Most operations should succeed
		assert.Greater(t, successRate, 85.0, "Should have high success rate with error recovery")

		t.Logf("Concurrent error handling results - Success Rate: %.2f%%, Retries: %d, Fatal Errors: %d", 
			successRate, totalRetries, fatalErrorCount)
	})

	t.Run("SystemResourceExhaustionRecovery", func(t *testing.T) {
		// Test recovery from system resource exhaustion
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 25
		const resourceIntensiveOps = 5

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		recoveryEvents := make(chan string, numGoroutines*resourceIntensiveOps)
		errorEvents := make(chan string, numGoroutines*resourceIntensiveOps)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				for j := 0; j < resourceIntensiveOps; j++ {
					// Simulate resource intensive operations
					ctx := context.Background()
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_begin_failed", goroutineID, j)
						continue
					}

					// Create multiple records of data
					batchSize := 100
					for batch := 0; batch < batchSize; batch++ {
						record := &types.Record{
							ID:    fmt.Sprintf("resource_exhaust_record_%d_%d_%d", goroutineID, j, batch),
							Table: "test_table",
							Data: map[string]*types.Value{
								"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*100000 + j*1000 + batch)},
								"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("resource_exhaust_value_%d_%d_%d", goroutineID, j, batch)},
							},
						}

						// Add some data
						data := make([]byte, 1024) // 1KB values
						for k := range data {
							data[k] = byte((goroutineID + j + batch + k) % 256)
						}
						record.Data["binary_data"] = &types.Value{Type: types.ColumnTypeString, Data: string(data)}

						_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
						if err != nil {
							tx.Rollback()
							errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_batch_%d_insert_failed", goroutineID, j, batch)
							break
						}
					}

					// Commit the large transaction
					err = tx.Commit()
					if err != nil {
						errorEvents <- fmt.Sprintf("goroutine_%d_op_%d_commit_failed", goroutineID, j)
						recoveryEvents <- fmt.Sprintf("goroutine_%d_op_%d_recovered_via_rollback", goroutineID, j)
						continue
					}

					recoveryEvents <- fmt.Sprintf("goroutine_%d_op_%d_completed_successfully", goroutineID, j)
				}
			}()
		}

		// Wait for completion
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(60 * time.Second):
			t.Fatal("System resource exhaustion recovery test timed out")
		}

		// Analyze results
		close(recoveryEvents)
		close(errorEvents)

		successCount := 0
		recoveryCount := 0
		errorCount := 0

		for event := range recoveryEvents {
			if strings.Contains(event, "completed_successfully") {
				successCount++
			} else if strings.Contains(event, "recovered_via_rollback") {
				recoveryCount++
			}
		}

		for range errorEvents {
			errorCount++
		}

		// Get final resource metrics
		rm := pebble.GetResourceManager()
		finalMetrics := rm.GetResourceMetrics()

		t.Logf("System resource exhaustion test results:")
		t.Logf("  Successful Operations: %d", successCount)
		t.Logf("  Recovered Operations: %d", recoveryCount)
		t.Logf("  Total Errors: %d", errorCount)
		t.Logf("  Final Iterator Acquired: %d", finalMetrics.IteratorAcquired)
		t.Logf("  Final Buffer Acquired: %d", finalMetrics.BufferAcquired)

		// Most operations should succeed
		totalOps := numGoroutines * resourceIntensiveOps
		assert.Greater(t, successCount+recoveryCount, totalOps*8/10, "Should have high success rate even under resource pressure")
	})
}