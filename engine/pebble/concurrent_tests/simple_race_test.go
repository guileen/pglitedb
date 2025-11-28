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
)

// TestSimpleRaceConditions tests for potential race conditions in concurrent operations
func TestSimpleRaceConditions(t *testing.T) {
	// Create test engine
	engine := createSimpleTestEngine(t)
	defer engine.Close()

	t.Run("ConcurrentAccessToSameKey", func(t *testing.T) {
		const numGoroutines = 20
		const numOperations = 50
		
		var wg sync.WaitGroup
		wg.Add(numGoroutines)
		
		errors := make(chan error, numGoroutines*numOperations)
		
		// Create a simple schema for testing
		schemaDef := &types.TableDefinition{
			ID:   "test_table_1",
			Name: "test_table",
			Columns: []types.ColumnDefinition{
				{
					Name:       "id",
					Type:       types.ColumnTypeBigInt,
					PrimaryKey: true,
				},
				{
					Name: "data",
					Type: types.ColumnTypeString,
				},
			},
		}
		
		for i := 0; i < numGoroutines; i++ {
			goroutineID := i
			go func() {
				defer wg.Done()
				
				for j := 0; j < numOperations; j++ {
					ctx := context.Background()
					
					// Randomly perform read or write operation
					if j%2 == 0 {
						// Read operation
						tx, err := engine.BeginTx(ctx)
						if err != nil {
							errors <- fmt.Errorf("goroutine %d: failed to begin tx for read: %w", goroutineID, err)
							return
						}
						
						// Try to read a row (may not exist, which is fine)
						_, err = tx.GetRow(ctx, 1, 1, int64(j%10), schemaDef)
						if err != nil && err != types.ErrRecordNotFound {
							tx.Rollback()
							errors <- fmt.Errorf("goroutine %d: failed to read row: %w", goroutineID, err)
							return
						}
						
						err = tx.Commit()
						if err != nil {
							errors <- fmt.Errorf("goroutine %d: failed to commit read tx: %w", goroutineID, err)
							return
						}
					} else {
						// Write operation
						tx, err := engine.BeginTx(ctx)
						if err != nil {
							errors <- fmt.Errorf("goroutine %d: failed to begin tx for write: %w", goroutineID, err)
							return
						}
						
						// Create a simple record
						record := &types.Record{
							ID:    fmt.Sprintf("row_%d_%d", goroutineID, j),
							Table: "test_table",
							Data: map[string]*types.Value{
								"id":   {Type: types.ColumnTypeBigInt, Data: int64(j % 10)},
								"data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("value_from_goroutine_%d_op_%d", goroutineID, j)},
							},
						}
						
						_, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
						if err != nil {
							tx.Rollback()
							errors <- fmt.Errorf("goroutine %d: failed to insert row: %w", goroutineID, err)
							return
						}
						
						err = tx.Commit()
						if err != nil {
							errors <- fmt.Errorf("goroutine %d: failed to commit write tx: %w", goroutineID, err)
							return
						}
					}
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
			t.Fatal("Race condition test timed out")
		}
		
		// Check for errors
		close(errors)
		errorCount := 0
		for err := range errors {
			t.Logf("Error: %v", err)
			errorCount++
		}
		
		assert.Equal(t, 0, errorCount, "Should have no errors in race condition test")
	})
}

// Helper function to create test engine
func createSimpleTestEngine(t *testing.T) engineTypes.StorageEngine {
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