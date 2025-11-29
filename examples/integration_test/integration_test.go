package main

import (
	"context"
	"testing"
	"time"

	"github.com/guileen/pglitedb/catalog"
	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

func TestBasicIntegration(t *testing.T) {
	// Create a temporary database for testing
	dbPath := "/tmp/pglitedb-integration-test-" + time.Now().Format("20060102150405")
	
	// Create storage
	config := storage.DefaultPebbleConfig(dbPath)
	kvStore, err := storage.NewPebbleKV(config)
	assert.NoError(t, err)
	defer kvStore.Close()
	
	// Create codec
	c := codec.NewMemComparableCodec()
	
	// Create engine
	eng := engine.NewStorageEngine(kvStore, c)
	
	// Create catalog manager
	mgr := catalog.NewTableManagerWithKV(eng, kvStore)
	
	// Test basic operations
	ctx := context.Background()
	
	// Create a table
	tableDef := &types.TableDefinition{
		Name: "users",
		Columns: []types.ColumnDefinition{
			{Name: "id", Type: types.ColumnTypeInteger, PrimaryKey: true},
			{Name: "name", Type: types.ColumnTypeText},
			{Name: "email", Type: types.ColumnTypeText},
		},
	}
	
	err = mgr.CreateTable(ctx, 1, tableDef)
	assert.NoError(t, err)
	
	// Insert a record
	values := map[string]interface{}{
		"id":    1,
		"name":  "John Doe",
		"email": "john@example.com",
	}
	
	_, err = mgr.InsertRow(ctx, 1, "users", values)
	assert.NoError(t, err)
	
	// This is a basic smoke test - we're just verifying that the basic
	// components can be instantiated and work together
	assert.True(t, true)
}