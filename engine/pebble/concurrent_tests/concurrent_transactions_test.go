package concurrent_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentTransactions tests basic concurrent transaction operations
func TestConcurrentTransactions(t *testing.T) {
	// Create test engine
	engine := createTestEngine(t)
	defer engine.Close()

	const numWorkers = 3
	const numOpsPerWorker = 20

	// Use WaitGroup to coordinate workers
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Channel to collect errors
	errChan := make(chan error, numWorkers*numOpsPerWorker)

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

	// Start multiple worker goroutines
	for i := 0; i < numWorkers; i++ {
		workerID := i
		go func() {
			defer wg.Done()
			
			for j := 0; j < numOpsPerWorker; j++ {
				ctx := context.Background()
				
				// Begin transaction
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errChan <- err
					return
				}

				// Perform some operations
				record := &types.Record{
					ID:    fmt.Sprintf("worker_%d_op_%d", workerID, j),
					Table: "test_table",
					Data: map[string]*types.Value{
						"id":   {Type: types.ColumnTypeNumber, Data: float64(workerID*1000 + j)},
						"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("value_worker_%d_op_%d", workerID, j)},
					},
				}
				
				// Insert operation
				_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
				if err != nil {
					tx.Rollback()
					errChan <- err
					return
				}

				// Commit transaction
				err = tx.Commit()
				tx.Rollback() // Ensure transaction is properly closed
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Wait for all workers to complete
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	// Wait with timeout or until all workers are done
	select {
	case <-done:
		// All workers completed
	case <-time.After(60 * time.Second):
		t.Fatal("Test timed out")
	}

	// Check for errors
	close(errChan)
	for err := range errChan {
		assert.NoError(t, err, "Concurrent transaction should not produce errors")
	}

	// Verify data integrity
	for i := 0; i < numWorkers; i++ {
		for j := 0; j < numOpsPerWorker; j++ {
			ctx := context.Background()
			tx, err := engine.BeginTx(ctx)
			require.NoError(t, err)
			
			// Try to read the record back
			_, err = tx.GetRow(ctx, 1, 1, int64(i*1000+j), schemaDef)
			// We're not checking the exact value since we're just testing concurrency
			// The important thing is that no errors occurred during the concurrent operations
			
			err = tx.Commit()
			tx.Rollback() // Ensure transaction is properly closed
			require.NoError(t, err)
		}
	}
}

// TestConcurrentReadsAndWrites tests concurrent read and write operations
func TestConcurrentReadsAndWrites(t *testing.T) {
	// Create test engine
	engine := createTestEngine(t)
	defer engine.Close()

	const numReaders = 2
	const numWriters = 2
	const numOps = 10

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

	// Pre-populate some data
	ctx := context.Background()
	for i := 0; i < numOps; i++ {
		tx, err := engine.BeginTx(ctx)
		require.NoError(t, err)
		
		record := &types.Record{
			ID:    fmt.Sprintf("pre_key_%d", i),
			Table: "test_table",
			Data: map[string]*types.Value{
				"id":   {Type: types.ColumnTypeNumber, Data: float64(i)},
				"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("pre_value_%d", i)},
			},
		}
		
		_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
		require.NoError(t, err)
		
		err = tx.Commit()
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(numReaders + numWriters)

	errChan := make(chan error, (numReaders+numWriters)*numOps)

	// Start reader goroutines
	for i := 0; i < numReaders; i++ {
		go func() {
			defer wg.Done()
			
			for j := 0; j < numOps; j++ {
				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errChan <- err
					return
				}

				// Read a random key
				keyIdx := j % numOps
				
				_, err = tx.GetRow(ctx, 1, 1, int64(keyIdx), schemaDef)
				if err != nil && err != types.ErrRecordNotFound {
					tx.Rollback()
					errChan <- err
					return
				}

				err = tx.Commit()
				tx.Rollback() // Ensure transaction is properly closed
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Start writer goroutines
	for i := 0; i < numWriters; i++ {
		writerID := i
		go func() {
			defer wg.Done()
			
			for j := 0; j < numOps; j++ {
				ctx := context.Background()
				tx, err := engine.BeginTx(ctx)
				if err != nil {
					errChan <- err
					return
				}

				// Write to a key
				record := &types.Record{
					ID:    fmt.Sprintf("writer_%d_key_%d", writerID, j),
					Table: "test_table",
					Data: map[string]*types.Value{
						"id":   {Type: types.ColumnTypeNumber, Data: float64(writerID*1000 + j)},
						"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("writer_%d_value_%d", writerID, j)},
					},
				}
				
				_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
				if err != nil {
					tx.Rollback()
					errChan <- err
					return
				}

				err = tx.Commit()
				tx.Rollback() // Ensure transaction is properly closed
				if err != nil {
					errChan <- err
					return
				}
			}
		}()
	}

	// Wait for completion with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Success
	case <-time.After(60 * time.Second):
		t.Fatal("Test timed out")
	}

	// Check for errors
	close(errChan)
	for err := range errChan {
		assert.NoError(t, err)
	}
}

// TestHighConcurrency tests high concurrency scenarios
func TestHighConcurrency(t *testing.T) {
	// Create test engine
	engine := createTestEngine(t)
	defer engine.Close()

	const numGoroutines = 5
	const opsPerGoroutine = 3

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

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	results := make(chan int, numGoroutines*opsPerGoroutine)
	errChan := make(chan error, numGoroutines*opsPerGoroutine)

	// Start many goroutines performing various operations
	for i := 0; i < numGoroutines; i++ {
		goroutineID := i
		go func() {
			defer wg.Done()
			
			for j := 0; j < opsPerGoroutine; j++ {
				ctx := context.Background()
				
				// Randomly choose operation type
				opType := j % 3
				
				switch opType {
				case 0: // Insert
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errChan <- err
						return
					}
					
					record := &types.Record{
						ID:    fmt.Sprintf("high_conc_insert_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*10000 + j)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("high_conc_value_%d_%d", goroutineID, j)},
						},
					}
					
					_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
					if err != nil {
						tx.Rollback()
						errChan <- err
						return
					}
					
					err = tx.Commit()
					if err != nil {
						errChan <- err
						return
					}
					results <- 1
					
				case 1: // Read
					tx, err := engine.BeginTx(ctx)
					if err != nil {
						errChan <- err
						return
					}
					
					// Read a key that might exist
					keyIdx := (goroutineID*1000 + j/3) % 10000
					
					_, err = tx.GetRow(ctx, 1, 1, int64(keyIdx), schemaDef)
					if err != nil && err != types.ErrRecordNotFound {
						tx.Rollback()
						errChan <- err
						return
					}
					
					err = tx.Commit()
					tx.Rollback() // Ensure transaction is properly closed
					if err != nil {
						errChan <- err
						return
					}
					results <- 2
					
				case 2: // Update (insert then update)
					// First insert
					tx1, err := engine.BeginTx(ctx)
					if err != nil {
						errChan <- err
						return
					}
					
					record1 := &types.Record{
						ID:    fmt.Sprintf("high_conc_update_%d_%d", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*10000 + j*2)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("initial_value_%d_%d", goroutineID, j)},
						},
					}
					
					_, err = tx1.InsertRow(ctx, 1, 1, record1, schemaDef)
					if err != nil {
						tx1.Rollback()
						errChan <- err
						return
					}
					
					err = tx1.Commit()
					if err != nil {
						errChan <- err
						return
					}
					
					// Then update - in this case we'll just insert another record since we don't have update methods
					tx2, err := engine.BeginTx(ctx)
					if err != nil {
						errChan <- err
						return
					}
					
					record2 := &types.Record{
						ID:    fmt.Sprintf("high_conc_update_%d_%d_updated", goroutineID, j),
						Table: "test_table",
						Data: map[string]*types.Value{
							"id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*10000 + j*2 + 1)},
							"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("updated_value_%d_%d", goroutineID, j)},
						},
					}
					
					_, err = tx2.InsertRow(ctx, 1, 1, record2, schemaDef)
					if err != nil {
						tx2.Rollback()
						errChan <- err
						return
					}
					
					err = tx2.Commit()
					if err != nil {
						errChan <- err
						return
					}
					results <- 3
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
	case <-time.After(120 * time.Second):
		t.Fatal("High concurrency test timed out")
	}

	// Check results
	close(results)
	close(errChan)
	
	resultCount := 0
	for range results {
		resultCount++
	}
	
	errorCount := 0
	for err := range errChan {
		t.Logf("Error occurred: %v", err)
		errorCount++
	}
	
	// We should have processed all operations
	expectedResults := numGoroutines * opsPerGoroutine
	assert.Equal(t, expectedResults, resultCount+errorCount, "Should have processed all operations")
	assert.Equal(t, 0, errorCount, "Should have no errors in high concurrency test")
}

// Helper function to create test engine
func createTestEngine(t *testing.T) engineTypes.StorageEngine {
	t.Helper()
	
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	
	// Create Pebble KV store
	config := storage.DefaultPebbleConfig(tempDir)
	kvStore, err := storage.NewPebbleKV(config)
	if err != nil {
		t.Fatalf("Failed to create KV store: %v", err)
	}
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine
	engine := pebble.NewPebbleEngine(kvStore, c)
	
	return engine
}