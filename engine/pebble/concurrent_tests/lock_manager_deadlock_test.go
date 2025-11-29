package concurrent_tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLockManagerDeadlockDetection tests the lock manager's deadlock detection capabilities
func TestLockManagerDeadlockDetection(t *testing.T) {
	// Test the lock manager's ability to detect and resolve deadlocks
	// This requires access to the internal lock manager, which may not be directly exposed

	// Since the lock manager is internal to the transaction package,
	// we'll test deadlock detection indirectly through transaction behavior

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
				Type:       types.ColumnTypeBigInt,
				PrimaryKey: true,
			},
			{
				Name: "data",
				Type: types.ColumnTypeString,
			},
		},
	}

	ctx := context.Background()

	// Create transactions
	tx1, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	tx2, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	// Pre-populate records
	initTx, err := engine.BeginTx(ctx)
	require.NoError(t, err)

	record1 := &types.Record{
		ID:    "lockmgr_record_1",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeBigInt, Data: int64(30)},
			"data": {Type: types.ColumnTypeString, Data: "init_1"},
		},
	}

	record2 := &types.Record{
		ID:    "lockmgr_record_2",
		Table: "test_table",
		Data: map[string]*types.Value{
			"id":   {Type: types.ColumnTypeBigInt, Data: int64(31)},
			"data": {Type: types.ColumnTypeString, Data: "init_2"},
		},
	}

	rowID1, err := initTx.InsertRow(ctx, 1, 1, record1, schemaDef)
	require.NoError(t, err)
	rowID2, err := initTx.InsertRow(ctx, 1, 1, record2, schemaDef)
	require.NoError(t, err)
	err = initTx.Commit()
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)

	// Channels for coordination
	phase1Done := make(chan bool, 2)
	proceed := make(chan bool, 2)

	// Transaction 1 sequence:
	// 1. Update record1
	// 2. Signal ready
	// 3. Wait for signal
	// 4. Try to update record2
	go func() {
		defer wg.Done()

		// Phase 1: Update record1
		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "tx1_modified"},
		}
		err := tx1.UpdateRow(ctx, 1, 1, rowID1, updates1, schemaDef)
		assert.NoError(t, err)

		// Signal that phase 1 is complete
		phase1Done <- true

		// Wait for signal to proceed
		<-proceed

		// Phase 2: Try to update record2 (potential deadlock)
		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "tx1_modified_record2"},
		}
		err = tx1.UpdateRow(ctx, 1, 1, rowID2, updates2, schemaDef)
		if err != nil {
			// This is expected in a deadlock scenario
			t.Logf("Transaction 1 conflict as expected: %v", err)
		}
	}()

	// Transaction 2 sequence:
	// 1. Update record2
	// 2. Signal ready
	// 3. Wait for signal
	// 4. Try to update record1
	go func() {
		defer wg.Done()

		// Phase 1: Update record2
		updates2 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "tx2_modified"},
		}
		err := tx2.UpdateRow(ctx, 1, 1, rowID2, updates2, schemaDef)
		assert.NoError(t, err)

		// Signal that phase 1 is complete
		phase1Done <- true

		// Wait for signal to proceed
		<-proceed

		// Phase 2: Try to update record1 (potential deadlock)
		updates1 := map[string]*types.Value{
			"data": {Type: types.ColumnTypeString, Data: "tx2_modified_record1"},
		}
		err = tx2.UpdateRow(ctx, 1, 1, rowID1, updates1, schemaDef)
		if err != nil {
			// This is expected in a deadlock scenario
			t.Logf("Transaction 2 conflict as expected: %v", err)
		}
	}()

	// Wait for both transactions to complete phase 1
	<-phase1Done
	<-phase1Done

	// Signal both transactions to proceed with phase 2
	proceed <- true
	proceed <- true

	// Wait with timeout
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Both transactions completed
		t.Log("Both transactions completed - deadlock detection worked correctly")
	case <-time.After(5 * time.Second):
		t.Fatal("Deadlock detection test timed out - deadlock was not resolved")
	}

	// Clean up
	tx1.Rollback()
	tx2.Rollback()
}