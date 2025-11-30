package client

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClientCreation(t *testing.T) {
	// This test just verifies that the client can be created without panicking
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Client creation panicked: %v", r)
		}
	}()

	tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	dbPath := tmpDir + "/test-db"
	client := NewClient(dbPath)
	if client == nil {
		t.Error("NewClient returned nil")
	}
}

func TestConvertFunctions(t *testing.T) {
	// Test convertExternalToInternalOptions
	externalOptions := &types.QueryOptions{
		Columns: []string{"id", "name"},
		Where: map[string]interface{}{
			"age": 30,
		},
		OrderBy: []string{"name ASC", "age DESC"},
		Limit:   intPtr(10),
		Offset:  intPtr(0),
	}

	internalOptions := convertExternalToInternalOptions(externalOptions)

	if len(internalOptions.Columns) != 2 {
		t.Errorf("Expected 2 columns, got %d", len(internalOptions.Columns))
	}

	if internalOptions.Where == nil {
		t.Error("Where map should not be nil")
	}

	if len(internalOptions.OrderBy) != 2 {
		t.Errorf("Expected 2 order by clauses, got %d", len(internalOptions.OrderBy))
	}

	if internalOptions.Limit == nil {
		t.Error("Limit should not be nil")
	} else if *internalOptions.Limit != 10 {
		t.Errorf("Expected limit 10, got %d", *internalOptions.Limit)
	}
}

func TestNewClientWithExecutor(t *testing.T) {
	t.Run("NewClientWithExecutorCreation", func(t *testing.T) {
		// Test that NewClientWithExecutor creates a valid instance
		client := NewClientWithExecutor(nil, nil)
		assert.NotNil(t, client)
		assert.Nil(t, client.executor)
		assert.Nil(t, client.planner)
	})
}

func TestConvertExternalToInternalOptions(t *testing.T) {
	t.Run("NilOptions", func(t *testing.T) {
		// Test converting nil options
		result := convertExternalToInternalOptions(nil)
		assert.NotNil(t, result)
	})

	t.Run("ExistingOptions", func(t *testing.T) {
		// Test converting existing options
		options := &types.QueryOptions{
			Columns: []string{"col1", "col2"},
			Where:   map[string]interface{}{"col1": "value1"},
			Limit:   intPtr(10),
		}

		result := convertExternalToInternalOptions(options)
		assert.Equal(t, options, result)
	})
}

func TestQuery(t *testing.T) {
	t.Run("ValidQueryString", func(t *testing.T) {
		// Test Query with valid string
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		result, err := client.Query(ctx, "SELECT 1")
		// This should succeed as it's a valid expression query
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(1), result.Count)
	})

	t.Run("InvalidQueryType", func(t *testing.T) {
		// Test Query with invalid type
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		_, err = client.Query(ctx, 123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query type")
	})

	t.Run("NilQuery", func(t *testing.T) {
		// Test Query with nil
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		_, err = client.Query(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query type")
	})
}

func TestExplain(t *testing.T) {
	t.Run("ValidQueryString", func(t *testing.T) {
		// Test Explain with valid string
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		result, err := client.Explain(ctx, "SELECT 1")
		// With the updated implementation, this should not return an error
		// but rather an execution plan
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("InvalidQueryType", func(t *testing.T) {
		// Test Explain with invalid type
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		_, err = client.Explain(ctx, 123)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query type")
	})

	t.Run("NilQuery", func(t *testing.T) {
		// Test Explain with nil
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		_, err = client.Explain(ctx, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid query type")
	})
}

func TestInsert(t *testing.T) {
	t.Run("InsertWithVariousDataTypes", func(t *testing.T) {
		// Test Insert with various data types
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		data := map[string]interface{}{
			"name":    "John Doe",
			"age":     30,
			"salary":  50000.50,
			"active":  true,
			"address": "123 Main St",
		}

		result, err := client.Insert(ctx, 1, "users", data)
		// With the updated implementation, this should not return a nil result
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("InsertWithEmptyData", func(t *testing.T) {
		// Test Insert with empty data
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		data := map[string]interface{}{}

		result, err := client.Insert(ctx, 1, "users", data)
		// With the updated implementation, this should not return a nil result
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestSelect(t *testing.T) {
	t.Run("SelectWithVariousOptions", func(t *testing.T) {
		// Test Select with various options
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		options := &types.QueryOptions{
			Columns: []string{"name", "age"},
			Where: map[string]interface{}{
				"age": 30,
			},
			OrderBy: []string{"name ASC"},
			Limit:   intPtr(10),
			Offset:  intPtr(0),
		}

		_, err = client.Select(ctx, 1, "users", options)
		// This should fail because the table doesn't exist
		if assert.NotNil(t, err, "Expected error for non-existent table") { // Only check error content if error is not nil
			assert.Contains(t, err.Error(), "not found")
		}
	})

	t.Run("SelectWithNilOptions", func(t *testing.T) {
		// Test Select with nil options
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		_, err = client.Select(ctx, 1, "users", nil)
		// This will likely fail because the table doesn't exist, but we're testing the method structure
		assert.NotNil(t, err) // Expected to fail due to table not existing
	})

	t.Run("SelectWithEmptyOptions", func(t *testing.T) {
		// Test Select with empty options
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		options := &types.QueryOptions{}

		_, err = client.Select(ctx, 1, "users", options)
		// This will likely fail because the table doesn't exist, but we're testing the method structure
		assert.NotNil(t, err) // Expected to fail due to table not existing
	})
}

func TestUpdate(t *testing.T) {
	t.Run("UpdateWithVariousDataTypes", func(t *testing.T) {
		// Test Update with various data types
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		data := map[string]interface{}{
			"name":   "Jane Doe",
			"age":    25,
			"salary": 60000.75,
			"active": false,
		}
		where := map[string]interface{}{
			"id": 1,
		}

		_, err = client.Update(ctx, 1, "users", data, where)
		// This should fail because the table doesn't exist
		if assert.NotNil(t, err, "Expected error for non-existent table") { // Only check error content if error is not nil
			assert.Contains(t, err.Error(), "not found")
		}
	})

	t.Run("UpdateWithEmptyData", func(t *testing.T) {
		// Test Update with empty data
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		data := map[string]interface{}{}
		where := map[string]interface{}{
			"id": 1,
		}

		_, err = client.Update(ctx, 1, "users", data, where)
		// This will likely fail, but we're testing the method structure
		assert.NotNil(t, err)
	})

	t.Run("UpdateWithNilWhere", func(t *testing.T) {
		// Test Update with nil where clause
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		data := map[string]interface{}{
			"name": "Jane Doe",
		}
		var where map[string]interface{}

		_, err = client.Update(ctx, 1, "users", data, where)
		// This should generate SQL without WHERE clause
		assert.NotNil(t, err) // Expected to fail due to table not existing
	})
}

func TestDelete(t *testing.T) {
	t.Run("DeleteWithWhereClause", func(t *testing.T) {
		// Test Delete with where clause
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		where := map[string]interface{}{
			"id": 1,
		}

		_, err = client.Delete(ctx, 1, "users", where)
		// This should fail because the table doesn't exist
		if assert.NotNil(t, err, "Expected error for non-existent table") { // Only check error content if error is not nil
			assert.Contains(t, err.Error(), "not found")
		}
	})

	t.Run("DeleteWithoutWhereClause", func(t *testing.T) {
		// Test Delete without where clause (deletes all rows)
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		var where map[string]interface{}

		_, err = client.Delete(ctx, 1, "users", where)
		// This should generate SQL that deletes all rows
		assert.NotNil(t, err) // Expected to fail due to table not existing
	})
}

func TestBatchInsert(t *testing.T) {
	t.Run("BatchInsertWithMultipleRecords", func(t *testing.T) {
		// Test BatchInsert with multiple records
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		dataList := []map[string]interface{}{
			{
				"name":   "John Doe",
				"age":    30,
				"salary": 50000.50,
				"active": true,
			},
			{
				"name":   "Jane Smith",
				"age":    25,
				"salary": 60000.75,
				"active": false,
			},
		}

		result, err := client.BatchInsert(ctx, 1, "users", dataList)
		// With the updated implementation, this should not return a nil result
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("BatchInsertWithEmptyList", func(t *testing.T) {
		// Test BatchInsert with empty list
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		var dataList []map[string]interface{}

		result, err := client.BatchInsert(ctx, 1, "users", dataList)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Equal(t, int64(0), result.Count)
	})

	t.Run("BatchInsertWithSingleRecord", func(t *testing.T) {
		// Test BatchInsert with single record
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		dataList := []map[string]interface{}{
			{
				"name":   "John Doe",
				"age":    30,
				"salary": 50000.50,
				"active": true,
			},
		}

		result, err := client.BatchInsert(ctx, 1, "users", dataList)
		// With the updated implementation, this should not return a nil result
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})
}

func TestClientStructure(t *testing.T) {
	t.Run("ClientFields", func(t *testing.T) {
		// Test that Client struct has the expected fields
		client := &Client{
			executor: nil,
			planner:  nil,
		}

		assert.Nil(t, client.executor)
		assert.Nil(t, client.planner)
	})
}

func TestConcurrentAccess(t *testing.T) {
	t.Run("ConcurrentClientOperations", func(t *testing.T) {
		// Test that client can handle concurrent operations
		tmpDir, err := ioutil.TempDir("", "pglitedb-client-test-*")
		require.NoError(t, err)
		defer os.RemoveAll(tmpDir)

		dbPath := tmpDir + "/test-db"
		client := NewClient(dbPath)

		ctx := context.Background()
		done := make(chan bool, 3)

		go func() {
			_, _ = client.Query(ctx, "SELECT 1")
			done <- true
		}()

		go func() {
			_, _ = client.Explain(ctx, "SELECT 1")
			done <- true
		}()

		go func() {
			data := map[string]interface{}{"name": "test"}
			_, _ = client.Insert(ctx, 1, "users", data)
			done <- true
		}()

		// Wait for all goroutines to complete
		for i := 0; i < 3; i++ {
			<-done
		}
	})
}

func intPtr(i int) *int {
	return &i
}