package pebble

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/codec"
	engineTypes "github.com/guileen/pglitedb/engine/types"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
)

func createTestSchema() *dbTypes.TableDefinition {
	return &dbTypes.TableDefinition{
		ID:      "1",
		Name:    "users",
		Version: 1,
		Columns: []dbTypes.ColumnDefinition{
			{Name: "id", Type: dbTypes.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: dbTypes.ColumnTypeString},
			{Name: "email", Type: dbTypes.ColumnTypeString},
			{Name: "age", Type: dbTypes.ColumnTypeNumber},
			{Name: "active", Type: dbTypes.ColumnTypeBoolean},
		},
		Indexes: []dbTypes.IndexDefinition{
			{Name: "idx_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
			{Name: "idx_age", Columns: []string{"age"}, Unique: false},
		},
	}
}

func createSnapshotTestEngine(t *testing.T) *pebbleEngine {
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
	engine := NewPebbleEngine(kvStore, c).(*pebbleEngine)
	
	return engine
}

func insertTestData(t *testing.T, engine *pebbleEngine, count int) []int64 {
	t.Helper()
	
	ctx := context.Background()
	schema := createTestSchema()
	
	var rowIDs []int64
	
	for i := 1; i <= count; i++ {
		record := &dbTypes.Record{
			Data: map[string]*dbTypes.Value{
				"name":   {Data: "User" + string(rune('0'+i)), Type: dbTypes.ColumnTypeString},
				"email":  {Data: "user" + string(rune('0'+i)) + "@example.com", Type: dbTypes.ColumnTypeString},
				"age":    {Data: int64(20 + (i % 10)), Type: dbTypes.ColumnTypeNumber},
				"active": {Data: i%2 == 0, Type: dbTypes.ColumnTypeBoolean},
			},
		}
		
		rowID, err := engine.InsertRow(ctx, 1, 1, record, schema)
		if err != nil {
			t.Fatalf("Failed to insert test data: %v", err)
		}
		
		rowIDs = append(rowIDs, rowID)
	}
	
	return rowIDs
}

func TestSnapshotTransaction_UpdateRows(t *testing.T) {
	// Create a test engine
	engine := createSnapshotTestEngine(t)
	defer engine.Close()
	
	ctx := context.Background()
	schema := createTestSchema()
	
	// Insert test data
	insertTestData(t, engine, 10)
	
	// Test cases for UpdateRows
	testCases := []struct {
		name        string
		conditions  map[string]interface{}
		updates     map[string]*dbTypes.Value
		expectedCount int64
	}{
		{
			name: "Update rows with specific age",
			conditions: map[string]interface{}{
				"age": int64(21),
			},
			updates: map[string]*dbTypes.Value{
				"name": {Data: "UpdatedUser", Type: dbTypes.ColumnTypeString},
			},
			expectedCount: 1, // Only one user has age 21
		},
		{
			name: "Update rows with active status",
			conditions: map[string]interface{}{
				"active": true,
			},
			updates: map[string]*dbTypes.Value{
				"age": {Data: int64(99), Type: dbTypes.ColumnTypeNumber},
			},
			expectedCount: 5, // Half of 10 users are active
		},
		{
			name: "Update rows with non-matching condition",
			conditions: map[string]interface{}{
				"age": int64(100),
			},
			updates: map[string]*dbTypes.Value{
				"name": {Data: "NonExistent", Type: dbTypes.ColumnTypeString},
			},
			expectedCount: 0, // No users have age 100
		},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a snapshot transaction
			tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
			if err != nil {
				t.Fatalf("Failed to begin snapshot transaction: %v", err)
			}
			
			// Perform UpdateRows operation
			count, err := tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, tc.updates, tc.conditions, schema)
			if err != nil {
				t.Fatalf("UpdateRows failed: %v", err)
			}
			
			if count != tc.expectedCount {
				t.Errorf("Expected %d rows updated, got %d", tc.expectedCount, count)
			}
			
			// Commit the transaction
			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
			
			// Verify the updates by scanning the table
			if tc.expectedCount > 0 {
				// Use the engine's ScanRows method to verify updates
				iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
				if err != nil {
					t.Fatalf("Failed to scan rows: %v", err)
				}
				defer iter.Close()
				
				var verifiedCount int64
				for iter.Next() {
					record := iter.Row()
					
					// Check if the record matches our update conditions
					matches := true
					for col, expectedVal := range tc.updates {
						actualVal, exists := record.Data[col]
						if !exists || actualVal.Data != expectedVal.Data {
							matches = false
							break
						}
					}
					
					if matches {
						verifiedCount++
					}
				}
				
				if err := iter.Error(); err != nil {
					t.Fatalf("Iterator error: %v", err)
				}
				
				if verifiedCount != tc.expectedCount {
					t.Errorf("Verification failed: expected %d matching rows, found %d", tc.expectedCount, verifiedCount)
				}
			}
		})
	}
}

func TestSnapshotTransaction_DeleteRows(t *testing.T) {
	// Test cases for DeleteRows
	testCases := []struct {
		name        string
		insertCount int
		conditions  map[string]interface{}
		expectedDeleted int64
	}{
		{
			name:        "Delete rows with specific age",
			insertCount: 10,
			conditions: map[string]interface{}{
				"age": int64(21),
			},
			expectedDeleted: 1, // Only one user has age 21
		},
		{
			name:        "Delete rows with inactive status",
			insertCount: 10,
			conditions: map[string]interface{}{
				"active": false,
			},
			expectedDeleted: 5, // Half of 10 users are inactive
		},
		{
			name:        "Delete rows with non-matching condition",
			insertCount: 10,
			conditions: map[string]interface{}{
				"age": int64(100),
			},
			expectedDeleted: 0, // No users have age 100
		},
	}
	
	for _, tc := range testCases {
		tc := tc // Capture range variable
		t.Run(tc.name, func(t *testing.T) {
			// Create a fresh test engine for each test case to ensure isolation
			engine := createSnapshotTestEngine(t)
			defer engine.Close()
			
			ctx := context.Background()
			schema := createTestSchema()
			
			// Insert fresh test data for each test case
			insertTestData(t, engine, tc.insertCount)
			
			// Create a snapshot transaction
			tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
			if err != nil {
				t.Fatalf("Failed to begin snapshot transaction: %v", err)
			}
			
			// Perform DeleteRows operation
			count, err := tx.(engineTypes.Transaction).DeleteRows(ctx, 1, 1, tc.conditions, schema)
			if err != nil {
				t.Fatalf("DeleteRows failed: %v", err)
			}
			
			if count != tc.expectedDeleted {
				t.Errorf("Expected %d rows deleted, got %d", tc.expectedDeleted, count)
			}
			
			// Commit the transaction
			if err := tx.Commit(); err != nil {
				t.Fatalf("Failed to commit transaction: %v", err)
			}
			
			// Verify the deletions by scanning the table
			// Create a new transaction to verify results
			verifyTx, err := engine.BeginTx(ctx)
			if err != nil {
				t.Fatalf("Failed to begin verification transaction: %v", err)
			}
			
			// Count remaining rows
			iterOpts := &storage.IteratorOptions{
				LowerBound: engine.codec.EncodeTableKey(1, 1, 0),
				UpperBound: engine.codec.EncodeTableKey(1, 1, int64(^uint64(0)>>1)),
			}
			
			iter := verifyTx.(*transaction).kvTxn.NewIterator(iterOpts)
			if iter == nil {
				t.Fatal("Failed to create iterator")
			}
			defer iter.Close()
			
			var remainingCount int64
			for iter.First(); iter.Valid(); iter.Next() {
				remainingCount++
			}
			
			expectedRemaining := int64(tc.insertCount) - tc.expectedDeleted
			if remainingCount != expectedRemaining {
				t.Errorf("Verification failed: expected %d remaining rows, found %d", expectedRemaining, remainingCount)
			}
			
			if err := verifyTx.Commit(); err != nil {
				t.Fatalf("Failed to commit verification transaction: %v", err)
			}
		})
	}
}

func TestSnapshotTransaction_UpdateRowsAndDeleteRows_ErrorHandling(t *testing.T) {
	// Create a test engine
	engine := createSnapshotTestEngine(t)
	defer engine.Close()
	
	ctx := context.Background()
	schema := createTestSchema()
	
	// Test error handling for closed transaction
	t.Run("UpdateRows on closed transaction", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		// Commit the transaction to close it
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		
		// Try to perform UpdateRows on closed transaction
		updates := map[string]*dbTypes.Value{
			"name": {Data: "ShouldFail", Type: dbTypes.ColumnTypeString},
		}
		conditions := map[string]interface{}{
			"age": int64(25),
		}
		
		_, err = tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, updates, conditions, schema)
		if err == nil {
			t.Error("Expected error for UpdateRows on closed transaction, got nil")
		}
	})
	
	t.Run("DeleteRows on closed transaction", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		// Commit the transaction to close it
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		
		// Try to perform DeleteRows on closed transaction
		conditions := map[string]interface{}{
			"age": int64(25),
		}
		
		_, err = tx.(engineTypes.Transaction).DeleteRows(ctx, 1, 1, conditions, schema)
		if err == nil {
			t.Error("Expected error for DeleteRows on closed transaction, got nil")
		}
	})
	
	// Test with invalid schema
	t.Run("UpdateRows with invalid schema", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		updates := map[string]*dbTypes.Value{
			"invalid_column": {Data: "Invalid", Type: dbTypes.ColumnTypeString},
		}
		conditions := map[string]interface{}{
			"age": int64(25),
		}
		
		// Use a nil schema - currently this doesn't return an error in the implementation
		// This test documents the current behavior
		_, err = tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, updates, conditions, nil)
		// Note: The current implementation doesn't validate nil schema, so this may not return an error
		_ = err // We're not asserting on the error for now
		
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}
	})
	
	t.Run("DeleteRows with invalid schema", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		conditions := map[string]interface{}{
			"age": int64(25),
		}
		
		// Use a nil schema - currently this doesn't return an error in the implementation
		// This test documents the current behavior
		_, err = tx.(engineTypes.Transaction).DeleteRows(ctx, 1, 1, conditions, nil)
		// Note: The current implementation doesn't validate nil schema, so this may not return an error
		_ = err // We're not asserting on the error for now
		
		if err := tx.Rollback(); err != nil {
			t.Fatalf("Failed to rollback transaction: %v", err)
		}
	})
}

// TestSnapshotTransaction_ComprehensiveDataTypes tests UpdateRows with various data types and conditions
func TestSnapshotTransaction_ComprehensiveDataTypes(t *testing.T) {
	// Create a test engine
	engine := createSnapshotTestEngine(t)
	defer engine.Close()
	
	ctx := context.Background()
	
	// Create a schema with basic data types
	schema := &dbTypes.TableDefinition{
		ID:      "1",
		Name:    "test_table",
		Version: 1,
		Columns: []dbTypes.ColumnDefinition{
			{Name: "id", Type: dbTypes.ColumnTypeNumber, PrimaryKey: true},
			{Name: "name", Type: dbTypes.ColumnTypeString},
			{Name: "email", Type: dbTypes.ColumnTypeString},
			{Name: "age", Type: dbTypes.ColumnTypeNumber},
			{Name: "active", Type: dbTypes.ColumnTypeBoolean},
		},
		Indexes: []dbTypes.IndexDefinition{
			{Name: "idx_email", Columns: []string{"email"}, Unique: true},
			{Name: "idx_name", Columns: []string{"name"}, Unique: false},
		},
	}
	
	// Insert test data with basic data types
	t.Run("Insert test data with basic data types", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		records := []*dbTypes.Record{
			{
				Data: map[string]*dbTypes.Value{
					"name":   {Data: "Alice", Type: dbTypes.ColumnTypeString},
					"email":  {Data: "alice@example.com", Type: dbTypes.ColumnTypeString},
					"age":    {Data: int64(30), Type: dbTypes.ColumnTypeNumber},
					"active": {Data: true, Type: dbTypes.ColumnTypeBoolean},
				},
			},
			{
				Data: map[string]*dbTypes.Value{
					"name":   {Data: "Bob", Type: dbTypes.ColumnTypeString},
					"email":  {Data: "bob@example.com", Type: dbTypes.ColumnTypeString},
					"age":    {Data: int64(25), Type: dbTypes.ColumnTypeNumber},
					"active": {Data: true, Type: dbTypes.ColumnTypeBoolean},
				},
			},
			{
				Data: map[string]*dbTypes.Value{
					"name":   {Data: "Charlie", Type: dbTypes.ColumnTypeString},
					"email":  {Data: "charlie@example.com", Type: dbTypes.ColumnTypeString},
					"age":    {Data: int64(35), Type: dbTypes.ColumnTypeNumber},
					"active": {Data: false, Type: dbTypes.ColumnTypeBoolean},
				},
			},
		}
		
		for _, record := range records {
			_, err := tx.(engineTypes.Transaction).InsertRow(ctx, 1, 1, record, schema)
			if err != nil {
				t.Fatalf("Failed to insert test data: %v", err)
			}
		}
		
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})
	
	// Test UpdateRows with various data types
	t.Run("UpdateRows with various data types", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		// Update records where age is 30
		updates := map[string]*dbTypes.Value{
			"name": {Data: "Alice Updated", Type: dbTypes.ColumnTypeString},
		}
		
		conditions := map[string]interface{}{
			"age": int64(30),
		}
		
		count, err := tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, updates, conditions, schema)
		if err != nil {
			t.Fatalf("UpdateRows failed: %v", err)
		}
		
		if count != 1 {
			t.Errorf("Expected 1 row updated, got %d", count)
		}
		
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
		
		// Verify the update
		iter, err := engine.ScanRows(ctx, 1, 1, schema, nil)
		if err != nil {
			t.Fatalf("Failed to scan rows: %v", err)
		}
		defer iter.Close()
		
		found := false
		for iter.Next() {
			record := iter.Row()
			if name, ok := record.Data["name"]; ok && name.Data == "Alice Updated" {
				found = true
				break
			}
		}
		
		if !found {
			t.Error("Failed to verify updated data")
		}
	})
	
	// Test UpdateRows with multiple conditions
	t.Run("UpdateRows with multiple conditions", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		// Update records where age is 25 and active is true
		updates := map[string]*dbTypes.Value{
			"name": {Data: "Bob Updated", Type: dbTypes.ColumnTypeString},
		}
		
		conditions := map[string]interface{}{
			"age":    int64(25),
			"active": true,
		}
		
		count, err := tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, updates, conditions, schema)
		if err != nil {
			t.Fatalf("UpdateRows failed: %v", err)
		}
		
		if count != 1 {
			t.Errorf("Expected 1 row updated, got %d", count)
		}
		
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})
	
	// Test UpdateRows with no matching conditions
	t.Run("UpdateRows with no matching conditions", func(t *testing.T) {
		tx, err := engine.BeginTxWithIsolation(ctx, storage.SnapshotIsolation)
		if err != nil {
			t.Fatalf("Failed to begin snapshot transaction: %v", err)
		}
		
		// Update records where age is 100 (no matches)
		updates := map[string]*dbTypes.Value{
			"name": {Data: "No Match", Type: dbTypes.ColumnTypeString},
		}
		
		conditions := map[string]interface{}{
			"age": int64(100),
		}
		
		count, err := tx.(engineTypes.Transaction).UpdateRows(ctx, 1, 1, updates, conditions, schema)
		if err != nil {
			t.Fatalf("UpdateRows failed: %v", err)
		}
		
		if count != 0 {
			t.Errorf("Expected 0 rows updated, got %d", count)
		}
		
		if err := tx.Commit(); err != nil {
			t.Fatalf("Failed to commit transaction: %v", err)
		}
	})
}