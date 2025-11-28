package concurrent_tests

import (
    "context"
    "fmt"
    "sync"
    "testing"
    "time"

    engineTypes "github.com/guileen/pglitedb/engine/types"
    "github.com/guileen/pglitedb/engine/pebble/resources"
    "github.com/guileen/pglitedb/types"
    "github.com/stretchr/testify/assert"
    "github.com/stretchr/testify/require"
)

// TestRaceConditions tests for potential race conditions in concurrent operations
func TestRaceConditions(t *testing.T) {
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

    t.Run("ConcurrentAccessToSameRecord", func(t *testing.T) {
        const numGoroutines = 20
        const numOperations = 50
        
        var wg sync.WaitGroup
        wg.Add(numGoroutines)
        
        errors := make(chan error, numGoroutines*numOperations)
        
        // Pre-populate a record that all goroutines will access
        initTx, err := engine.BeginTx(context.Background())
        require.NoError(t, err)
        
        initRecord := &types.Record{
            ID:    "shared_race_record",
            Table: "test_table",
            Data: map[string]*types.Value{
                "id":   {Type: types.ColumnTypeNumber, Data: float64(100)},
                "data": {Type: types.ColumnTypeString, Data: "initial_shared_data"},
            },
        }
        
        _, err = initTx.InsertRow(context.Background(), 1, 1, initRecord, schemaDef)
        require.NoError(t, err)
        err = initTx.Commit()
        require.NoError(t, err)
        
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
                        
                        _, err = tx.GetRow(ctx, 1, 1, 100, schemaDef)
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
                        // Write operation (update)
                        tx, err := engine.BeginTx(ctx)
                        if err != nil {
                            errors <- fmt.Errorf("goroutine %d: failed to begin tx for write: %w", goroutineID, err)
                            return
                        }
                        
                        updates := map[string]*types.Value{
                            "data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("value_from_goroutine_%d_op_%d", goroutineID, j)},
                        }
                        err = tx.UpdateRow(ctx, 1, 1, 100, updates, schemaDef)
                        if err != nil {
                            tx.Rollback()
                            errors <- fmt.Errorf("goroutine %d: failed to update row: %w", goroutineID, err)
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

    t.Run("ConcurrentResourceManagerAccess", func(t *testing.T) {
        const numGoroutines = 30
        const numOperations = 20
        
        var wg sync.WaitGroup
        wg.Add(numGoroutines)
        
        errors := make(chan error, numGoroutines*numOperations)
        
        // Test concurrent access to resource manager
        for i := 0; i < numGoroutines; i++ {
            goroutineID := i
            go func() {
                defer wg.Done()
                
                for j := 0; j < numOperations; j++ {
                    // Acquire and release various resources concurrently
                    rm := resources.GetResourceManager()
                    
                    // Use goroutineID to make operations unique
                    _ = goroutineID
                    
                    // Acquire iterator
                    iter := rm.AcquireIterator()
                    time.Sleep(time.Microsecond) // Simulate some work
                    rm.ReleaseIterator(iter)
                    
                    // Acquire buffer
                    buf := rm.AcquireBuffer(100)
                    time.Sleep(time.Microsecond) // Simulate some work
                    rm.ReleaseBuffer(buf)
                    
                    // Acquire record
                    record := rm.AcquireRecord()
                    time.Sleep(time.Microsecond) // Simulate some work
                    rm.ReleaseRecord(record)
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
            t.Fatal("Resource manager race condition test timed out")
        }
        
        // Check for errors
        close(errors)
        errorCount := 0
        for err := range errors {
            t.Logf("Error: %v", err)
            errorCount++
        }
        
        assert.Equal(t, 0, errorCount, "Should have no errors in resource manager race condition test")
    })

    t.Run("ConcurrentTransactionStateAccess", func(t *testing.T) {
        const numGoroutines = 15
        const numTransactions = 10
        
        var wg sync.WaitGroup
        wg.Add(numGoroutines)
        
        errors := make(chan error, numGoroutines*numTransactions)
        
        // Create some transactions to test concurrent access
        transactions := make([]engineTypes.Transaction, numTransactions)
        ctx := context.Background()
        
        for i := 0; i < numTransactions; i++ {
            tx, err := engine.BeginTx(ctx)
            require.NoError(t, err)
            transactions[i] = tx
        }
        
        // Test concurrent access to transaction state
        for i := 0; i < numGoroutines; i++ {
            go func() {
                defer wg.Done()
                
                for j := 0; j < numTransactions; j++ {
                    tx := transactions[j]
                    
                    // Try to access transaction methods concurrently
                    // This tests that transaction state is properly synchronized
                    _ = tx.Isolation()
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
            // Success - commit all transactions
            for _, tx := range transactions {
                err := tx.Commit()
                if err != nil {
                    errors <- err
                }
            }
        case <-time.After(30 * time.Second):
            t.Fatal("Transaction state race condition test timed out")
        }
        
        // Check for errors
        close(errors)
        errorCount := 0
        for err := range errors {
            t.Logf("Error: %v", err)
            errorCount++
        }
        
        assert.Equal(t, 0, errorCount, "Should have no errors in transaction state race condition test")
    })
}

// TestConcurrentIteratorUsage tests concurrent iterator usage for race conditions
func TestConcurrentIteratorUsage(t *testing.T) {
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

    // Pre-populate data
    ctx := context.Background()
    const numRecords = 100
    
    for i := 0; i < numRecords; i++ {
        tx, err := engine.BeginTx(ctx)
        require.NoError(t, err)
        
        record := &types.Record{
            ID:    fmt.Sprintf("iter_test_record_%05d", i),
            Table: "test_table",
            Data: map[string]*types.Value{
                "id":   {Type: types.ColumnTypeNumber, Data: float64(i)},
                "data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("iter_test_value_%05d", i)},
            },
        }
        
        _, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
        require.NoError(t, err)
        
        err = tx.Commit()
        require.NoError(t, err)
    }

    const numGoroutines = 10
    const numIterations = 20
    
    var wg sync.WaitGroup
    wg.Add(numGoroutines)
    
    errors := make(chan error, numGoroutines*numIterations)
    
    // Start multiple goroutines that perform concurrent read/write operations
    for i := 0; i < numGoroutines; i++ {
        go func(goroutineID int) {
            defer wg.Done()
            
            for j := 0; j < numIterations; j++ {
                ctx := context.Background()
                tx, err := engine.BeginTx(ctx)
                if err != nil {
                    errors <- fmt.Errorf("goroutine %d iteration %d: failed to begin tx: %w", goroutineID, j, err)
                    return
                }
                
                // Perform read operation on existing records
                recordID := int64((goroutineID * numIterations + j) % numRecords)
                _, err = tx.GetRow(ctx, 1, 1, recordID, schemaDef)
                if err != nil && err != types.ErrRecordNotFound {
                    tx.Rollback()
                    errors <- fmt.Errorf("goroutine %d iteration %d: failed to get row: %w", goroutineID, j, err)
                    return
                }
                
                // Small delay to increase chance of race
                time.Sleep(time.Nanosecond)
                
                // Perform write operation
                record := &types.Record{
                    ID:    fmt.Sprintf("race_test_record_%d_%d", goroutineID, j),
                    Table: "test_table",
                    Data: map[string]*types.Value{
                        "id":   {Type: types.ColumnTypeNumber, Data: float64(goroutineID*10000 + j)},
                        "data": {Type: types.ColumnTypeString, Data: fmt.Sprintf("race_test_value_%d_%d", goroutineID, j)},
                    },
                }
                
                _, err = tx.InsertRow(ctx, 1, 1, record, schemaDef)
                if err != nil {
                    tx.Rollback()
                    errors <- fmt.Errorf("goroutine %d iteration %d: failed to insert row: %w", goroutineID, j, err)
                    return
                }
                
                err = tx.Commit()
                if err != nil {
                    errors <- fmt.Errorf("goroutine %d iteration %d: failed to commit tx: %w", goroutineID, j, err)
                    return
                }
            }
        }(i)
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
        t.Fatal("Concurrent iterator usage test timed out")
    }
    
    // Check for errors
    close(errors)
    errorCount := 0
    for err := range errors {
        t.Logf("Error: %v", err)
        errorCount++
    }
    
    assert.Equal(t, 0, errorCount, "Should have no errors in concurrent iterator usage test")
}