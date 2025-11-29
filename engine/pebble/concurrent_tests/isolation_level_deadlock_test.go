package concurrent_tests

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDeadlockWithDifferentIsolationLevels tests deadlock scenarios with different isolation levels
func TestDeadlockWithDifferentIsolationLevels(t *testing.T) {
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

	// Test deadlock scenarios with different isolation levels

	ctx := context.Background()

	// Test with ReadCommitted vs RepeatableRead
	tx1, err := engine.BeginTxWithIsolation(ctx, storage.ReadCommitted)
	require.NoError(t, err)

	tx2, err := engine.BeginTxWithIsolation(ctx, storage.RepeatableRead)
	require.NoError(t, err)

	// Pre-populate records
	initTx, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	record1 := &types.Record{
		ID:    "isolation_deadlock_1",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(20)},
			"data": {Type: types.ColumnTypeString, Data: "initial_1"},
		},
	}

	record2 := &types.Record{
		ID:    "isolation_deadlock_2",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeNumber, Data: float64(21)},
			"data": {Type: types.ColumnTypeString, Data: "initial_2"},
		},
	}

	_, err = initTx.InsertRow(ctx, 1, 1, record1, schemaDef)
	require.NoError(t, err)
	_, err = initTx.InsertRow(ctx, 1, 1, record2, schemaDef)
	require.NoError(t, err)
	err = initTx.Commit()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	errors := make(chan error, 2)
	results := make(chan string, 2)

	// Transaction 1 (ReadCommitted): update record1, then try record2
	go func() {
		defer wg.Done()

		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_rc_tx"},
		}
		err := tx1.UpdateRow(ctx, 1, 1, 20, updates1, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("ReadCommitted tx failed to update record1: %w", err)
			return
		}

		time.Sleep(10 * time.Millisecond)

		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_rc_tx"},
		}
		err = tx1.UpdateRow(ctx, 1, 1, 21, updates2, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("ReadCommitted tx failed to update record2: %w", err)
			results <- "rc_tx_conflict"
			return
		}

		results <- "rc_tx_success"
	}()

	// Transaction 2 (RepeatableRead): update record2, then try record1
	go func() {
		defer wg.Done()

		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_rr_tx"},
		}
		err := tx2.UpdateRow(ctx, 1, 1, 21, updates2, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("RepeatableRead tx failed to update record2: %w", err)
			return
		}

		time.Sleep(10 * time.Millisecond)

		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "modified_by_rr_tx"},
		}
		err = tx2.UpdateRow(ctx, 1, 1, 20, updates1, schemaDef)
		if err != nil {
			errors <- fmt.Errorf("RepeatableRead tx failed to update record1: %w", err)
			results <- "rr_tx_conflict"
			return
		}

		results <- "rr_tx_success"
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
		t.Fatal("Isolation level deadlock test timed out")
	}

	// Check results
	close(errors)
	close(results)

	conflictCount := 0

	for result := range results {
		if result == "rc_tx_conflict" || result == "rr_tx_conflict" {
			conflictCount++
		}
	}

	// At least one transaction should experience a conflict
	assert.True(t, conflictCount > 0, "At least one transaction should experience a conflict with different isolation levels")

	// Clean up
	tx1.Rollback()
	tx2.Rollback()
}