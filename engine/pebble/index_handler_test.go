package pebble

import (
	"context"
	"testing"

	"github.com/guileen/pglitedb/codec"
	"github.com/guileen/pglitedb/storage"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

func TestIndexHandler_CreateIndex(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	assert.NoError(t, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	handler := NewIndexHandler(kvStore, c)
	
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)
	
	indexDef := &dbTypes.IndexDefinition{
		Name:    "test_index",
		Columns: []string{"col1", "col2"},
		Unique:  false,
		Type:    "btree",
	}
	
	// Mock nextIndexID function
	nextIndexID := func(ctx context.Context, tenantID, tableID int64) (int64, error) {
		return 1, nil
	}
	
	// Test CreateIndex
	err = handler.CreateIndex(ctx, tenantID, tableID, indexDef, nextIndexID)
	assert.NoError(t, err)
}

func TestIndexHandler_DropIndex(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	assert.NoError(t, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	handler := NewIndexHandler(kvStore, c)
	
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)
	indexID := int64(1)
	
	// Test DropIndex
	err = handler.DropIndex(ctx, tenantID, tableID, indexID)
	assert.NoError(t, err)
}

func TestIndexHandler_LookupIndex(t *testing.T) {
	// Setup
	tmpDir := t.TempDir()
	config := storage.DefaultPebbleConfig(tmpDir)
	kvStore, err := storage.NewPebbleKV(config)
	assert.NoError(t, err)
	defer kvStore.Close()

	c := codec.NewMemComparableCodec()
	handler := NewIndexHandler(kvStore, c)
	
	ctx := context.Background()
	tenantID := int64(1)
	tableID := int64(1)
	indexID := int64(1)
	
	// Test LookupIndex - should return empty slice instead of error when index doesn't exist
	rowIDs, err := handler.LookupIndex(ctx, tenantID, tableID, indexID, "test_value")
	assert.NoError(t, err)
	// Should be empty slice when no matching index entries found (not nil)
	assert.Empty(t, rowIDs)
}