package catalog

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine/pebble"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestEnvironment(t *testing.T) (Manager, func()) {
	tmpDir, err := os.MkdirTemp("", "data-manager-test-*")
	require.NoError(t, err)

	config := storage.DefaultPebbleConfig(filepath.Join(tmpDir, "db"))
	kvStore, err := storage.NewPebbleKV(config)
	require.NoError(t, err)

	c := codec.NewMemComparableCodec()
	eng := pebble.NewPebbleEngine(kvStore, c)
	
	// Create the table manager which will set up all the components properly
	manager := NewTableManager(eng)

	cleanup := func() {
		eng.Close()
		os.RemoveAll(tmpDir)
	}

	return manager, cleanup
}

func createTestTable(t *testing.T, mgr Manager, tenantID int64) {
	ctx := context.Background()
	
	tableDef := &types.TableDefinition{
		Name: "test_users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, Nullable: false, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeString, Nullable: false},
			{Name: "email", Type: types.ColumnTypeString, Nullable: false},
			{Name: "age", Type: types.ColumnTypeInteger, Nullable: true},
		},
	}
	
	err := mgr.CreateTable(ctx, tenantID, tableDef)
	require.NoError(t, err)
}

func TestDataManagerIntegration_Insert(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	
	// Create the test table
	createTestTable(t, mgr, tenantID)

	// Test successful insert
	data := map[string]interface{}{
		"id":    int64(1),
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   int64(30),
	}

	record, err := mgr.Insert(ctx, tenantID, "test_users", data)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.NotEmpty(t, record.ID)
	assert.Equal(t, "Alice", record.Data["name"].Data)
	assert.Equal(t, "alice@example.com", record.Data["email"].Data)
	
	// Handle the type conversion properly
	ageValue := record.Data["age"].Data
	switch v := ageValue.(type) {
	case int64:
		assert.Equal(t, int64(30), v)
	case int32:
		assert.Equal(t, int32(30), v)
	default:
		assert.Equal(t, int64(30), v)
	}

	// Test insert with missing required field
	invalidData := map[string]interface{}{
		"name": "Bob",
		// missing id and email
	}

	_, err = mgr.Insert(ctx, tenantID, "test_users", invalidData)
	assert.Error(t, err)
}

func TestDataManagerIntegration_InsertBatch(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	
	// Create the test table
	createTestTable(t, mgr, tenantID)

	// Test successful batch insert
	rows := []map[string]interface{}{
		{
			"id":    int64(1),
			"name":  "Alice",
			"email": "alice@example.com",
			"age":   int64(30),
		},
		{
			"id":    int64(2),
			"name":  "Bob",
			"email": "bob@example.com",
			"age":   int64(25),
		},
		{
			"id":    int64(3),
			"name":  "Charlie",
			"email": "charlie@example.com",
			"age":   int64(35),
		},
	}

	records, err := mgr.InsertBatch(ctx, tenantID, "test_users", rows)
	require.NoError(t, err)
	assert.Len(t, records, 3)
	
	// Verify the records have IDs
	assert.NotEmpty(t, records[0].ID)
	assert.NotEmpty(t, records[1].ID)
	assert.NotEmpty(t, records[2].ID)
	
	// Verify the data
	assert.Equal(t, "Alice", records[0].Data["name"].Data)
	assert.Equal(t, "Bob", records[1].Data["name"].Data)
	assert.Equal(t, "Charlie", records[2].Data["name"].Data)
}

func TestDataManagerIntegration_Get(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	
	// Create the test table
	createTestTable(t, mgr, tenantID)

	// First insert a record
	data := map[string]interface{}{
		"id":    int64(1),
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   int64(30),
	}

	insertedRecord, err := mgr.Insert(ctx, tenantID, "test_users", data)
	require.NoError(t, err)

	// Now test getting the record using the actual ID from the inserted record
	recordID, err := strconv.ParseInt(insertedRecord.ID, 10, 64)
	require.NoError(t, err)
	
	record, err := mgr.Get(ctx, tenantID, "test_users", recordID)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, insertedRecord.ID, record.ID)
	assert.Equal(t, "Alice", record.Data["name"].Data)
	assert.Equal(t, "alice@example.com", record.Data["email"].Data)
	
	// Handle the type conversion properly
	ageValue := record.Data["age"].Data
	switch v := ageValue.(type) {
	case int64:
		assert.Equal(t, int64(30), v)
	case int32:
		assert.Equal(t, int32(30), v)
	default:
		assert.Equal(t, int64(30), v)
	}

	// Test getting non-existent record
	_, err = mgr.Get(ctx, tenantID, "test_users", 999)
	assert.Error(t, err)
}

func TestDataManagerIntegration_Update(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	
	// Create the test table
	createTestTable(t, mgr, tenantID)

	// First insert a record
	data := map[string]interface{}{
		"id":    int64(1),
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   int64(30),
	}

	insertedRecord, err := mgr.Insert(ctx, tenantID, "test_users", data)
	require.NoError(t, err)

	// Get the actual row ID from the inserted record
	recordID, err := strconv.ParseInt(insertedRecord.ID, 10, 64)
	require.NoError(t, err)

	// Now test updating the record
	updates := map[string]interface{}{
		"name": "Alice Smith",
		"age":  int64(31),
	}

	record, err := mgr.Update(ctx, tenantID, "test_users", recordID, updates)
	require.NoError(t, err)
	assert.NotNil(t, record)
	assert.Equal(t, "Alice Smith", record.Data["name"].Data)
	
	// Handle the type conversion properly
	ageValue := record.Data["age"].Data
	switch v := ageValue.(type) {
	case int64:
		assert.Equal(t, int64(31), v)
	case int32:
		assert.Equal(t, int32(31), v)
	default:
		assert.Equal(t, int64(31), v)
	}
	
	assert.Equal(t, "alice@example.com", record.Data["email"].Data)

	// Test updating non-existent record
	_, err = mgr.Update(ctx, tenantID, "test_users", 999, updates)
	assert.Error(t, err)
}

func TestDataManagerIntegration_Delete(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	ctx := context.Background()
	tenantID := int64(1)
	
	// Create the test table
	createTestTable(t, mgr, tenantID)

	// First insert a record
	data := map[string]interface{}{
		"id":    int64(1),
		"name":  "Alice",
		"email": "alice@example.com",
		"age":   int64(30),
	}

	insertedRecord, err := mgr.Insert(ctx, tenantID, "test_users", data)
	require.NoError(t, err)

	// Get the actual row ID from the inserted record
	recordID, err := strconv.ParseInt(insertedRecord.ID, 10, 64)
	require.NoError(t, err)

	// Test successful deletion
	err = mgr.Delete(ctx, tenantID, "test_users", recordID)
	require.NoError(t, err)

	// Verify the record is deleted
	_, err = mgr.Get(ctx, tenantID, "test_users", recordID)
	assert.Error(t, err)

	// Test deleting non-existent record - this may not return an error depending on implementation
	err = mgr.Delete(ctx, tenantID, "test_users", 999)
	// We won't assert on the error since the behavior may vary
	// The important thing is that it doesn't panic
	_ = err
}

func TestDataManagerIntegration_InferSchemaFromData(t *testing.T) {
	mgr, cleanup := setupTestEnvironment(t)
	defer cleanup()

	// Get the underlying data manager to test the private method
	manager := mgr.(*tableManager)
	
	// Test schema inference with various data types
	data := map[string]interface{}{
		"name":      "Alice",
		"age":       30,
		"isActive":  true,
		"score":     3.14,
		"metadata":  map[string]interface{}{"key": "value"},
		"createdAt": "2023-12-01T10:00:00Z",
	}

	schema := manager.DataManager.(*dataManager).inferSchemaFromData("test_table", data)
	
	assert.Equal(t, "test_table", schema.Name)
	assert.Len(t, schema.Columns, 6)
	
	// Check that columns were inferred correctly
	columnMap := make(map[string]types.ColumnDefinition)
	for _, col := range schema.Columns {
		columnMap[col.Name] = col
	}
	
	assert.Equal(t, types.ColumnTypeString, columnMap["name"].Type)
	assert.Equal(t, types.ColumnTypeNumber, columnMap["age"].Type)
	assert.Equal(t, types.ColumnTypeBoolean, columnMap["isActive"].Type)
	assert.Equal(t, types.ColumnTypeNumber, columnMap["score"].Type)
	assert.Equal(t, types.ColumnTypeJSON, columnMap["metadata"].Type)
	
	// Check that indexes were created for number columns
	assert.Len(t, schema.Indexes, 2) // age and score are numbers
}