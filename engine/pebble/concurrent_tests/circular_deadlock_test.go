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

// TestCircularDeadlock tests a circular deadlock scenario with three transactions
func TestCircularDeadlock(t *testing.T) {
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

	// This test simulates a circular deadlock with three transactions:
	// Transaction A -> B -> C -> A

	ctx := context.Background()

	// Create three transactions
	tx1, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	tx2, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	tx3, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Pre-populate records
	recordA := &types.Record{
		ID:    "circular_deadlock_A",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(10)},
			"data": {Type: types.ColumnTypeString, Data: "initial_A"},
		},
	}

	recordB := &types.Record{
		ID:    "circular_deadlock_B",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(11)},
			"data": {Type: types.ColumnTypeString, Data: "initial_B"},
		},
	}

	recordC := &types.Record{
		ID:    "circular_deadlock_C",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(12)},
			"data": {Type: types.ColumnTypeString, Data: "initial_C"},
		},
	}

	// Insert initial records
	rowID_A, err := tx1.InsertRow(ctx, 1, 1, recordA, schemaDef)
	require.NoError(t, err)
	rowID_B, err := tx1.InsertRow(ctx, 1, 1, recordB, schemaDef)
	require.NoError(t, err)
	rowID_C, err := tx1.InsertRow(ctx, 1, 1, recordC, schemaDef)
	require.NoError(t, err)
	err = tx1.Commit()
	require.NoError(t, err)

	// Start new transactions for circular deadlock test
	tx1, err = engine.BeginTx(ctx)
	require.NoError(t, err)

	tx2, err = engine.BeginTx(ctx)
	require.NoError(t, err)

	tx3, err = engine.BeginTx(ctx)
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(3)

	errors := make(chan error, 3)
	results := make(chan string, 3)

	// Transaction 1: update record A, then try B
	go func() {
		defer wg.Done()

		updatesA := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx1"},
		}
		err := tx1.UpdateRow(ctx, 1, 1, rowID_A, updatesA, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx1 failed to update record A: %w", err)
			return
		}

		time.Sleep(5 * time.Millisecond)

		updatesB := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx1"},
		}
		err = tx1.UpdateRow(ctx, 1, 1, rowID_B, updatesB, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx1 failed to update record B: %w", err)
			results <- "tx1_conflict"
			return
		}

		results <- "tx1_success"
	}()

	// Transaction 2: update record B, then try C
	go func() {
		defer wg.Done()

		updatesB := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx2"},
		}
		err := tx2.UpdateRow(ctx, 1, 1, rowID_B, updatesB, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx2 failed to update record B: %w", err)
			return
		}

		time.Sleep(5 * time.Millisecond)

		updatesC := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx2"},
		}
		err = tx2.UpdateRow(ctx, 1, 1, rowID_C, updatesC, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx2 failed to update record C: %w", err)
			results <- "tx2_conflict"
			return
		}

		results <- "tx2_success"
	}()

	// Transaction 3: update record C, then try A
	go func() {
		defer wg.Done()

		updatesC := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx3"},
		}
		err := tx3.UpdateRow(ctx, 1, 1, rowID_C, updatesC, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx3 failed to update record C: %w", err)
			return
		}

		time.Sleep(5 * time.Millisecond)

		updatesA := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_tx3"},
		}
		err = tx3.UpdateRow(ctx, 1, 1, rowID_A, updatesA, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("tx3 failed to update record A: %w", err)
			results <- "tx3_conflict"
			return
		}

		results <- "tx3_success"
	}()

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// All goroutines completed
	case <-time.After(5 * time.Second):
		t.Fatal("Circular deadlock test timed out - possible actual deadlock")
	}

	// Check results
	close(errors)
	close(results)

	conflictCount := 0
	successCount := 0

	for result := range results {
		if result == "tx1_conflict" || result == "tx2_conflict" || result == "tx3_conflict" {
			conflictCount++
		} else if result == "tx1_success" || result == "tx2_success" || result == "tx3_success" {
			successCount++
		}
	}

	// At least one transaction should experience a conflict
	assert.True(t, conflictCount > 0, "At least one transaction should experience a conflict in circular deadlock scenario")

	// Clean up
	tx1.Rollback()
	tx2.Rollback()
	tx3.Rollback()
}