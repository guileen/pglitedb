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

// TestSimpleTwoWayDeadlock tests a classic two-way deadlock scenario
func TestSimpleTwoWayDeadlock(t *testing.T) {
	// Create test engine
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

	// This test simulates a classic two-way deadlock:
	// Transaction A locks resource 1, then tries to lock resource 2
	// Transaction B locks resource 2, then tries to lock resource 1

	ctx := context.Background()

	// Create two transactions
	tx1, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	tx2, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Pre-populate records
	record1 := &types.Record{
		ID:    "deadlock_record_1",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(1)},
			"data": {Type: types.ColumnTypeString, Data: "initial_1"},
		},
	}

	record2 := &types.Record{
		ID:    "deadlock_record_2",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(2)},
			"data": {Type: types.ColumnTypeString, Data: "initial_2"},
		},
	}

	// Insert initial records
	_, err = tx1.InsertRow(ctx, 1, 1, record1, schemaDef)
	require.NoError(t, err)
	_, err = tx1.InsertRow(ctx, 1, 1, record2, schemaDef)
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)

	// Start new transactions for deadlock test
	tx1, err = engine.BeginTx(ctx)
	require.NoError(t, err)

	tx2, err = engine.BeginTx(ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	errors := make(chan error, 2)
	results := make(chan string, 2)

	// Transaction A: update record1, then try to update record2
	go func() {
		defer wg.Done()

		// Update record1
		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx1"},
		}
		err := tx1.UpdateRow(ctx, 1, 1, 1, updates1, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx1 failed to update record1: %w", err)
			return
		}

		// Brief pause to ensure interleaving
		time.Sleep(10 * time.Millisecond)

		// Try to update record2
		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx1"},
		}
		err = tx1.UpdateRow(ctx, 1, 1, 2, updates2, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx1 failed to update record2: %w", err)
			results <- "tx1_conflict"
			return
		}

		results <- "tx1_success"
	}()

	// Transaction B: update record2, then try to update record1
	go func() {
		defer wg.Done()

		// Update record2
		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx2"},
		}
		err := tx2.UpdateRow(ctx, 1, 1, 2, updates2, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx2 failed to update record2: %w", err)
			return
		}

		// Brief pause to ensure interleaving
		time.Sleep(10 * time.Millisecond)

		// Try to update record1
		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx2"},
		}
		err = tx2.UpdateRow(ctx, 1, 1, 1, updates1, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx2 failed to update record1: %w", err)
			results <- "tx2_conflict"
			return
		}

		results <- "tx2_success"
	}()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Both goroutines completed
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock test timed out - possible actual deadlock")
	}

	// Check results
	close(errors)
	close(results)

	conflictCount := 0
	successCount := 0

	for result := range results {
		if result == "tx1_conflict" || result == "tx2_conflict" {
			conflictCount++
		} else if result == "tx1_success" || result == "tx2_success" {
			successCount++
		}
	}

	// At least one transaction should experience a conflict
	assert.True(t, conflictCount > 0, "At least one transaction should experience a conflict in deadlock scenario")

	// Clean up - rollback both transactions
	tx1.Rollback()
	tx2.Rollback()
}