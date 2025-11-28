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

// TestDeadlockScenarios tests various deadlock scenarios
func TestDeadlockScenarios(t *testing.T) {
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

	t.Run("SimpleTwoWayDeadlock", func(t *testing.T) {
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
	})

	t.Run("CircularDeadlock", func(t *testing.T) {
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
		_, err = tx1.InsertRow(ctx, 1, 1, recordA, schemaDef)
		require.NoError(t, err)
		_, err = tx1.InsertRow(ctx, 1, 1, recordB, schemaDef)
		require.NoError(t, err)
		_, err = tx1.InsertRow(ctx, 1, 1, recordC, schemaDef)
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
			err := tx1.UpdateRow(ctx, 1, 1, 10, updatesA, schemaDef)
			if err != nil {
				errors <- fmt.Errorf("tx1 failed to update record A: %w", err)
				return
			}
			
			time.Sleep(5 * time.Millisecond)
			
			updatesB := map[string]*types.Value{
				"data": {Type: types.ColumnTypeString, Data: "modified_by_tx1"},
			}
			err = tx1.UpdateRow(ctx, 1, 1, 11, updatesB, schemaDef)
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
			err := tx2.UpdateRow(ctx, 1, 1, 11, updatesB, schemaDef)
			if err != nil {
				errors <- fmt.Errorf("tx2 failed to update record B: %w", err)
				return
			}
			
			time.Sleep(5 * time.Millisecond)
			
			updatesC := map[string]*types.Value{
				"data": {Type: types.ColumnTypeString, Data: "modified_by_tx2"},
			}
			err = tx2.UpdateRow(ctx, 1, 1, 12, updatesC, schemaDef)
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
			err := tx3.UpdateRow(ctx, 1, 1, 12, updatesC, schemaDef)
			if err != nil {
				errors <- fmt.Errorf("tx3 failed to update record C: %w", err)
				return
			}
			
			time.Sleep(5 * time.Millisecond)
			
			updatesA := map[string]*types.Value{
				"data": {Type: types.ColumnTypeString, Data: "modified_by_tx3"},
			}
			err = tx3.UpdateRow(ctx, 1, 1, 10, updatesA, schemaDef)
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
	})

	t.Run("DeadlockWithDifferentIsolationLevels", func(t *testing.T) {
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
	})
}

// TestLockManagerDeadlockDetection tests the lock manager's deadlock detection capabilities
func TestLockManagerDeadlockDetection(t *testing.T) {
	// Test the lock manager's ability to detect and resolve deadlocks
	// This requires access to the internal lock manager, which may not be directly exposed
	
	// Since the lock manager is internal to the transaction package,
	// we'll test deadlock detection indirectly through transaction behavior
	
	t.Run("IndirectDeadlockDetection", func(t *testing.T) {
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
				"id":   {Type: types.ColumnTypeNumber, Data: float64(30)},
				"data": {Type: types.ColumnTypeString, Data: "init_1"},
			},
		}
		
		record2 := &types.Record{
			ID:    "lockmgr_record_2",
			Table: "test_table",
			Data: map[string]*types.Value{
				"id":   {Type: types.ColumnTypeNumber, Data: float64(31)},
				"data": {Type: types.ColumnTypeString, Data: "init_2"},
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
			err := tx1.UpdateRow(ctx, 1, 1, 30, updates1, schemaDef)
			assert.NoError(t, err)
			
			// Signal that phase 1 is complete
			phase1Done <- true
			
			// Wait for signal to proceed
			<-proceed
			
			// Phase 2: Try to update record2 (potential deadlock)
			updates2 := map[string]*types.Value{
				"data": {Type: types.ColumnTypeString, Data: "tx1_modified_record2"},
			}
			err = tx1.UpdateRow(ctx, 1, 1, 31, updates2, schemaDef)
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
			err := tx2.UpdateRow(ctx, 1, 1, 31, updates2, schemaDef)
			assert.NoError(t, err)
			
			// Signal that phase 1 is complete
			phase1Done <- true
			
			// Wait for signal to proceed
			<-proceed
			
			// Phase 2: Try to update record1 (potential deadlock)
			updates1 := map[string]*types.Value{
				"data": {Type: types.ColumnTypeString, Data: "tx2_modified_record1"},
			}
			err = tx2.UpdateRow(ctx, 1, 1, 30, updates1, schemaDef)
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
	})
}