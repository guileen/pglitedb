package catalog

import (
	"context"
	"strconv"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
)

type tableManager struct {
	SchemaManager
	DataManager
	QueryManager
	IndexManager

	engine engine.StorageEngine
	cache  *internal.SchemaCache
}

func NewTableManager(eng engine.StorageEngine) Manager {
	cache := internal.NewSchemaCache()
	sm := newSchemaManager(eng, nil, cache)
	return &tableManager{
		SchemaManager: sm,
		DataManager:   newDataManager(eng, cache, sm),
		QueryManager:  newQueryManager(eng, cache),
		IndexManager:  newIndexManager(eng, nil, cache),
		engine:        eng,
		cache:         cache,
	}
}

func NewTableManagerWithKV(eng engine.StorageEngine, kv storage.KV) Manager {
	cache := internal.NewSchemaCache()
	sm := newSchemaManager(eng, kv, cache)
	return &tableManager{
		SchemaManager: sm,
		DataManager:   newDataManager(eng, cache, sm),
		QueryManager:  newQueryManager(eng, cache),
		IndexManager:  newIndexManager(eng, kv, cache),
		engine:        eng,
		cache:         cache,
	}
}

// Implement the additional methods required by the Manager interface
func (tm *tableManager) InsertRow(ctx context.Context, tenantID int64, tableName string, values map[string]interface{}) (int64, error) {
	// Delegate to DataManager's Insert method
	record, err := tm.DataManager.Insert(ctx, tenantID, tableName, values)
	if err != nil {
		return 0, err
	}
	
	// Convert the record ID to int64
	// Note: This assumes the ID is stored as a string in the record
	// We might need to adjust this based on the actual implementation
	id, err := strconv.ParseInt(record.ID, 10, 64)
	if err != nil {
		return 0, err
	}
	
	return id, nil
}

func (tm *tableManager) UpdateRows(ctx context.Context, tenantID int64, tableName string, values map[string]interface{}, conditions map[string]interface{}) (int64, error) {
	// Delegate to DataManager's Update method
	return tm.DataManager.UpdateRows(ctx, tenantID, tableName, values, conditions)
}

func (tm *tableManager) DeleteRows(ctx context.Context, tenantID int64, tableName string, conditions map[string]interface{}) (int64, error) {
	// Delegate to DataManager's Delete method
	return tm.DataManager.DeleteRows(ctx, tenantID, tableName, conditions)
}