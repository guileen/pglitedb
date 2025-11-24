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
