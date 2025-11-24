package catalog

import (
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
	return &tableManager{
		SchemaManager: newSchemaManager(eng, nil, cache),
		DataManager:   newDataManager(eng, cache),
		QueryManager:  newQueryManager(eng, cache),
		IndexManager:  newIndexManager(eng, nil, cache),
		engine:        eng,
		cache:         cache,
	}
}

func NewTableManagerWithKV(eng engine.StorageEngine, kv storage.KV) Manager {
	cache := internal.NewSchemaCache()
	return &tableManager{
		SchemaManager: newSchemaManager(eng, kv, cache),
		DataManager:   newDataManager(eng, cache),
		QueryManager:  newQueryManager(eng, cache),
		IndexManager:  newIndexManager(eng, kv, cache),
		engine:        eng,
		cache:         cache,
	}
}
