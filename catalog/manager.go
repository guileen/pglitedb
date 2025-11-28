package catalog

import (
	"context"
	"strconv"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/catalog/system"
	"github.com/guileen/pglitedb/catalog/system/interfaces"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type tableManager struct {
	SchemaManager
	DataManager
	QueryManager
	IndexManager

	engine engine.StorageEngine
	cache  *internal.SchemaCache
	statsCollector interfaces.StatsManager
	systemCatalog system.SystemCatalog
}

// GetEngine returns the storage engine
func (tm *tableManager) GetEngine() engine.StorageEngine {
	return tm.engine
}

func NewTableManager(eng engine.StorageEngine) Manager {
	cache := internal.NewSchemaCache()
	sm := newSchemaManager(eng, nil, cache)
	tm := &tableManager{
		SchemaManager: sm,
		DataManager:   newDataManager(eng, cache, sm),
		QueryManager:  newQueryManager(eng, cache, nil), // Will be set below
		IndexManager:  newIndexManager(eng, nil, cache),
		engine:        eng,
		cache:         cache,
	}
	// Set the manager reference in QueryManager
	tm.QueryManager = newQueryManager(eng, cache, tm)
	tm.statsCollector = NewStatsCollector(interfaces.TableManager(tm))
	tm.systemCatalog = system.NewCatalog(interfaces.TableManager(tm))
	return tm
}

func NewTableManagerWithKV(eng engine.StorageEngine, kv storage.KV) Manager {
	cache := internal.NewSchemaCache()
	sm := newSchemaManager(eng, kv, cache)
	tm := &tableManager{
		SchemaManager: sm,
		DataManager:   newDataManager(eng, cache, sm),
		QueryManager:  newQueryManager(eng, cache, nil), // Will be set below
		IndexManager:  newIndexManager(eng, kv, cache),
		engine:        eng,
		cache:         cache,
	}
	// Set the manager reference in QueryManager
	tm.QueryManager = newQueryManager(eng, cache, tm)
	tm.statsCollector = NewStatsCollector(interfaces.TableManager(tm))
	tm.systemCatalog = system.NewCatalog(interfaces.TableManager(tm))
	return tm
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

// GetStatsCollector returns the statistics collector for this manager
func (tm *tableManager) GetStatsCollector() interfaces.StatsManager {
	return tm.statsCollector
}

// QuerySystemTable implements the Manager interface
func (tm *tableManager) QuerySystemTable(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	return tm.systemCatalog.QuerySystemTable(ctx, fullTableName, filter)
}

// SystemTableQuery implements the Manager interface
func (tm *tableManager) SystemTableQuery(ctx context.Context, fullTableName string, filter map[string]interface{}) (*types.QueryResult, error) {
	return tm.systemCatalog.QuerySystemTable(ctx, fullTableName, filter)
}

// Implement view management methods by delegating to SchemaManager
func (tm *tableManager) CreateView(ctx context.Context, tenantID int64, viewName string, query string, replace bool) error {
	return tm.SchemaManager.CreateView(ctx, tenantID, viewName, query, replace)
}

func (tm *tableManager) DropView(ctx context.Context, tenantID int64, viewName string) error {
	return tm.SchemaManager.DropView(ctx, tenantID, viewName)
}

func (tm *tableManager) GetViewDefinition(ctx context.Context, tenantID int64, viewName string) (*types.ViewDefinition, error) {
	return tm.SchemaManager.GetViewDefinition(ctx, tenantID, viewName)
}

// Implement constraint validation by delegating to SchemaManager
func (tm *tableManager) ValidateConstraint(ctx context.Context, tenantID int64, tableName string, constraint *types.ConstraintDef) error {
	return tm.SchemaManager.ValidateConstraint(ctx, tenantID, tableName, constraint)
}