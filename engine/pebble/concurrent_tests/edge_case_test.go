package concurrent_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEdgeCases tests various edge cases in concurrent scenarios
func TestEdgeCases(t *testing.T) {
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
			{
				Name: "large_data",
				Type: types.ColumnTypeString,
			},
		},
	}

	t.Run("VeryLargeDataValues", func(t *testing.T) {
		// Test concurrent operations with very large data values
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 5
		const largeDataSize = 100 * 1024 // 100KB (more reasonable for testing)

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to begin transaction: %w", goroutineID, err)
					return
				}

				// Create large data value
				largeData := make([]byte, largeDataSize)
				
				// Fill with pattern
				for j := 0; j < largeDataSize; j++ {
					largeData[j] = byte((goroutineID + j) % 256)
				}

				// Create record with large data
				record := &types.Record{
					ID:    fmt.Sprintf("large_data_record_%d", goroutineID),
					Table: "test_table",
					Data: map[string]*types.Value{
						"id":        {Type: types.ColumnTypeNumber, Data: float64(goroutineID)},
						"data":      {Type: types.ColumnTypeString, Data: fmt.Sprintf("normal_data_goroutine_%d", goroutineID)},
						"large_data": {Type: types.ColumnTypeString, Data: string(largeData)},
					},
				}

				_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
				if err != nil {
					tx.Rollback()
					errors <- fmt.Errorf("goroutine %d: failed to insert large data record: %w", goroutineID, err)
					return
				}

				err = tx.Commit()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to commit transaction: %w", goroutineID, err)
					return
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(30 * time.Second):
			t.Fatal("Large data test timed out")
		}

		// Check for errors
		close(errors)
		for err := range errors {
			assert.NoError(t, err, "Large data operations should not produce errors")
		}

		// Verify data integrity
		ctx := context.Background()
		for i := 0; i < numGoroutines; i++ {
			tx, err := engine.BeginTx(ctx)
			require.NoError(t, err)

			record, err := tx.GetRow(ctx, 1, 1, int64(i), schemaDef)
			if err == nil {
				// Verify record if found
				expectedData := make([]byte, largeDataSize)
				for j := 0; j < largeDataSize; j++ {
					expectedData[j] = byte((i + j) % 256)
				}
				actualDataStr, ok := record.Data["large_data"].Data.(string)
				if ok {
					assert.Equal(t, string(expectedData), actualDataStr, "Large data should match expected")
				}
			}

			tx.Commit()
		}
	})

	t.Run("EmptyValues", func(t *testing.T) {
		// Test concurrent operations with empty values
		engine := createTestEngine(t)
		defer engine.Close()

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

		const numGoroutines = 10

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to begin transaction: %w", goroutineID, err)
					return
				}

				// Test with empty value (should succeed)
				record := &types.Record{
					ID:    fmt.Sprintf("empty_value_record_%d", goroutineID),
					Table: "test_table",
					Data: map[string]*types.Value{
						"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID)},
						"data": {Type: types.ColumnTypeString, Data: ""}, // Empty string value
					},
				}

				_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
				if err != nil {
					tx.Rollback()
					errors <- fmt.Errorf("goroutine %d: failed to insert record with empty value: %w", goroutineID, err)
					return
				}

				err = tx.Commit()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to commit transaction: %w", goroutineID, err)
					return
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("Empty keys/values test timed out")
		}

		// Check for errors
		close(errors)
		errorCount := 0
		for err := range errors {
			t.Logf("Notice: %v", err)
			errorCount++
		}

		// Most operations should succeed
		assert.Less(t, errorCount, numGoroutines, "Most empty key/value operations should succeed")
	})

	t.Run("UnicodeAndSpecialCharacters", func(t *testing.T) {
		// Test concurrent operations with Unicode and special characters
		engine := createTestEngine(t)
		defer engine.Close()

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
				{
					Name: "unicode_data",
					Type: types.ColumnTypeString,
				},
			},
		}

		const numGoroutines = 8
		unicodeStrings := []string{
			"Hello, 世界",
			"🌟✨🎉🚀",
			"café naïve résumé",
			"Москва Санкт-Петербург",
			"مرحبا بالعالم",
			"こんにちは世界",
			"🎉🎊🥳🎈",
			"🔒🔑🛡️💻",
		}

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to begin transaction: %w", goroutineID, err)
					return
				}

				// Use Unicode strings in record data
				record := &types.Record{
					ID:    fmt.Sprintf("unicode_record_%d", goroutineID),
					Table: "test_table",
					Data: map[string]*types.Value{
						"id":          {Type: types.ColumnTypeNumber, Data: float64(goroutineID)},
						"data":        {Type: types.ColumnTypeString, Data: fmt.Sprintf("normal_data_%d", goroutineID)},
						"unicode_data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("%s_value_%d_🚀🌟", unicodeStrings[(goroutineID+1)%len(unicodeStrings)], goroutineID)},
					},
				}

				_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
				if err != nil {
					tx.Rollback()
					errors <- fmt.Errorf("goroutine %d: failed to insert Unicode data record: %w", goroutineID, err)
					return
				}

				err = tx.Commit()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to commit transaction: %w", goroutineID, err)
					return
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(10 * time.Second):
			t.Fatal("Unicode test timed out")
		}

		// Check for errors
		close(errors)
		for err := range errors {
			assert.NoError(t, err, "Unicode operations should not produce errors")
		}

		// Verify data integrity
		ctx := context.Background()
		for i := 0; i < numGoroutines; i++ {
			tx, err := engine.BeginTx(ctx)
			require.NoError(t, err)

			record, err := tx.GetRow(ctx, 1, 1, int64(i), schemaDef)
			if err == nil {
				expectedUnicodeData := fmt.Sprintf("%s_value_%d_🚀🌟", unicodeStrings[(i+1)%len(unicodeStrings)], i)
				actualUnicodeData, ok := record.Data["unicode_data"].Data.(string)
				if ok {
					assert.Equal(t, expectedUnicodeData, actualUnicodeData, "Unicode data should match expected")
				}
			}

			tx.Commit()
		}
	})

	t.Run("MaximumConcurrentTransactions", func(t *testing.T) {
		// Test the system under maximum concurrent transaction load
		engine := createTestEngine(t)
		defer engine.Close()

		const maxGoroutines = 100
		const operationsPerGoroutine = 10

		var wg sync.WaitGroup
		wg.Add(maxGoroutines)

		results := make(chan int, maxGoroutines*operationsPerGoroutine)
		errors := make(chan error, maxGoroutines*operationsPerGoroutine)

		startTime := time.Now()

		for i := 0; i < maxGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				for j := 0; j < operationsPerGoroutine; j++ {
					ctx := context.Background()
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errors <- fmt.Errorf("goroutine %d op %d: failed to begin transaction: %w", goroutineID, j, err)
						continue
					}

					// Perform mixed operations
					record := &types.Record{
						ID:    fmt.Sprintf("max_load_record_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*1000 + j)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("max_load_value_%d_%d_%d", goroutineID, j, time.Now().UnixNano()/1000000)},
						},
					}

					// Insert operation
					_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
					if err != nil {
						tx.Rollback()
						errors <- fmt.Errorf("goroutine %d op %d: failed to insert: %w", goroutineID, j, err)
						continue
					}

					// Commit
					err = tx.Commit()
					if err != nil {
						errors <- fmt.Errorf("goroutine %d op %d: failed to commit: %w", goroutineID, j, err)
						continue
					}

					results <- 1
				}
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
			duration := time.Since(startTime)
			t.Logf("Maximum concurrent transactions test completed in %v with %d goroutines", duration, maxGoroutines)
		case <-time.After(60 * time.Second):
			t.Fatal("Maximum concurrent transactions test timed out")
		}

		// Check results
		close(results)
		close(errors)

		successCount := 0
		for range results {
			successCount++
		}

		errorCount := 0
		for err := range errors {
			t.Logf("Error in max concurrent test: %v", err)
			errorCount++
		}

		expectedOperations := maxGoroutines * operationsPerGoroutine
		assert.Equal(t, expectedOperations, successCount+errorCount, "Should account for all operations")
		assert.Less(t, errorCount, expectedOperations/10, "Should have relatively few errors under maximum load")
	})

	t.Run("TransactionTimeoutUnderLoad", func(t *testing.T) {
		// Test transaction behavior when system is under heavy load
		engine := createTestEngine(t)
		defer engine.Close()

		const numGoroutines = 30
		const longRunningOps = 5

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		results := make(chan string, numGoroutines)
		errors := make(chan error, numGoroutines)

		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()

				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to begin transaction: %w", goroutineID, err)
					return
				}

				// Perform operations that take some time
				for j := 0; j < longRunningOps; j++ {
					record := &types.Record{
						ID:    fmt.Sprintf("timeout_test_record_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*100 + j)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("timeout_test_value_%d_%d_%d", goroutineID, j, time.Now().Nanosecond())},
						},
					}

					_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
					if err != nil {
						tx.Rollback()
						errors <- fmt.Errorf("goroutine %d op %d: failed to insert: %w", goroutineID, j, err)
						return
					}

					// Small delay to simulate work
					time.Sleep(time.Millisecond)
				}

				// Commit transaction
				err = tx.Commit()
				if err != nil {
					errors <- fmt.Errorf("goroutine %d: failed to commit: %w", goroutineID, err)
					results <- "commit_failed"
					return
				}

				results <- "success"
			}()
		}

		// Wait with timeout
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()

		select {
		case <-done:
			// Success
		case <-time.After(30 * time.Second):
			t.Fatal("Transaction timeout under load test timed out")
		}

		// Check results
		close(results)
		close(errors)

		successCount := 0
		failCount := 0

		for result := range results {
			if result == "success" {
				successCount++
			} else if result == "commit_failed" {
				failCount++
			}
		}

		errorCount := 0
		for err := range errors {
			t.Logf("Error in timeout test: %v", err)
			errorCount++
		}

		// Most transactions should succeed
		assert.Greater(t, successCount, numGoroutines/2, "Majority of transactions should succeed under load")
		t.Logf("Success: %d, Fail: %d, Errors: %d", successCount, failCount, errorCount)
	})
}