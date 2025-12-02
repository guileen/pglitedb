package internal

import (
	"testing"
	"github.com/stretchr/testify/assert"
	"github.com/guileen/pglitedb/types"
)

func TestSchemaCache_NewSchemaCache(t *testing.T) {
	cache := NewSchemaCache()
	assert.NotNil(t, cache)
}

func TestSchemaCache_SetAndGetTable(t *testing.T) {
	cache := NewSchemaCache()
	
	// Create a test table definition
	tableDef := &types.TableDefinition{
		Name: "test_table",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: "INTEGER"},
			{Name: "name", Type: "VARCHAR(255)"},
		},
	}
	
	tableID := int64(123)
	key := "test_key"
	
	// Set the table definition
	cache.Set(key, tableDef, tableID)
	
	// Get the table definition
	retrievedDef, retrievedID, ok := cache.Get(key)
	assert.True(t, ok)
	assert.Equal(t, tableDef, retrievedDef)
	assert.Equal(t, tableID, retrievedID)
	
	// Verify it's not a view
	_, ok = cache.GetView(key)
	assert.False(t, ok)
}

func TestSchemaCache_SetAndGetView(t *testing.T) {
	cache := NewSchemaCache()
	
	// Create a test view definition
	viewDef := &types.ViewDefinition{
		Name:  "test_view",
		Query: "SELECT * FROM test_table",
	}
	
	viewID := int64(456)
	key := "test_view_key"
	
	// Set the view definition
	cache.Set(key, viewDef, viewID)
	
	// Get the view definition
	retrievedDef, ok := cache.GetView(key)
	assert.True(t, ok)
	assert.Equal(t, viewDef, retrievedDef)
	
	// Verify it's not a table
	_, _, ok = cache.Get(key)
	assert.False(t, ok)
}

func TestSchemaCache_Delete(t *testing.T) {
	cache := NewSchemaCache()
	
	// Set a table definition
	tableDef := &types.TableDefinition{Name: "test_table"}
	cache.Set("test_key", tableDef, 123)
	
	// Verify it exists
	_, _, ok := cache.Get("test_key")
	assert.True(t, ok)
	
	// Delete it
	cache.Delete("test_key")
	
	// Verify it's gone
	_, _, ok = cache.Get("test_key")
	assert.False(t, ok)
	
	// Also verify view deletion
	viewDef := &types.ViewDefinition{Name: "test_view"}
	cache.Set("test_view_key", viewDef, 456)
	
	_, ok = cache.GetView("test_view_key")
	assert.True(t, ok)
	
	cache.Delete("test_view_key")
	
	_, ok = cache.GetView("test_view_key")
	assert.False(t, ok)
}

func TestSchemaCache_Exists(t *testing.T) {
	cache := NewSchemaCache()
	
	// Test non-existent key
	assert.False(t, cache.Exists("non_existent"))
	
	// Set a table definition
	tableDef := &types.TableDefinition{Name: "test_table"}
	cache.Set("test_key", tableDef, 123)
	
	// Test existing key
	assert.True(t, cache.Exists("test_key"))
	
	// Test deleted key
	cache.Delete("test_key")
	assert.False(t, cache.Exists("test_key"))
}

func TestSchemaCache_Range(t *testing.T) {
	cache := NewSchemaCache()
	
	// Set multiple table definitions
	table1 := &types.TableDefinition{Name: "table1"}
	table2 := &types.TableDefinition{Name: "table2"}
	view1 := &types.ViewDefinition{Name: "view1", Query: "SELECT * FROM table1"}
	
	cache.Set("table1", table1, 1)
	cache.Set("table2", table2, 2)
	cache.Set("view1", view1, 3)
	
	// Count tables using Range
	tableCount := 0
	cache.Range(func(key string, schema *types.TableDefinition, tableID int64) bool {
		tableCount++
		return true
	})
	
	assert.Equal(t, 2, tableCount)
	
	// Collect table keys
	var tableKeys []string
	cache.Range(func(key string, schema *types.TableDefinition, tableID int64) bool {
		tableKeys = append(tableKeys, key)
		return true
	})
	
	assert.Contains(t, tableKeys, "table1")
	assert.Contains(t, tableKeys, "table2")
	assert.Len(t, tableKeys, 2)
}

func TestSchemaCache_RangeViews(t *testing.T) {
	cache := NewSchemaCache()
	
	// Set multiple definitions including views
	table1 := &types.TableDefinition{Name: "table1"}
	view1 := &types.ViewDefinition{Name: "view1", Query: "SELECT * FROM table1"}
	view2 := &types.ViewDefinition{Name: "view2", Query: "SELECT COUNT(*) FROM table1"}
	
	cache.Set("table1", table1, 1)
	cache.Set("view1", view1, 2)
	cache.Set("view2", view2, 3)
	
	// Count views using RangeViews
	viewCount := 0
	cache.RangeViews(func(key string, view *types.ViewDefinition, viewID int64) bool {
		viewCount++
		return true
	})
	
	assert.Equal(t, 2, viewCount)
	
	// Collect view keys
	var viewKeys []string
	cache.RangeViews(func(key string, view *types.ViewDefinition, viewID int64) bool {
		viewKeys = append(viewKeys, key)
		return true
	})
	
	assert.Contains(t, viewKeys, "view1")
	assert.Contains(t, viewKeys, "view2")
	assert.Len(t, viewKeys, 2)
}

func TestSchemaCache_GetWrongType(t *testing.T) {
	cache := NewSchemaCache()
	
	// Set a table definition
	tableDef := &types.TableDefinition{Name: "test_table"}
	cache.Set("test_key", tableDef, 123)
	
	// Try to get it as a view (should fail)
	_, ok := cache.GetView("test_key")
	assert.False(t, ok)
	
	// Set a view definition
	viewDef := &types.ViewDefinition{Name: "test_view", Query: "SELECT * FROM test_table"}
	cache.Set("view_key", viewDef, 456)
	
	// Try to get it as a table (should fail)
	_, _, ok = cache.Get("view_key")
	assert.False(t, ok)
}