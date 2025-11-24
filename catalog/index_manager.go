package catalog

import (
	"context"
	"fmt"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

type indexManager struct {
	engine engine.StorageEngine
	kv     storage.KV
	cache  *internal.SchemaCache
}

func newIndexManager(eng engine.StorageEngine, kv storage.KV, cache *internal.SchemaCache) IndexManager {
	return &indexManager{
		engine: eng,
		kv:     kv,
		cache:  cache,
	}
}

func (m *indexManager) CreateIndex(ctx context.Context, tenantID int64, tableName string, indexDef *types.IndexDefinition) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	if err := m.engine.CreateIndex(ctx, tenantID, tableID, indexDef); err != nil {
		return fmt.Errorf("create index: %w", err)
	}

	newSchema := *schema
	newSchema.Indexes = append(newSchema.Indexes, *indexDef)

	key := makeTableKey(tenantID, tableName)
	m.cache.Set(key, &newSchema, tableID)

	return nil
}

func (m *indexManager) DropIndex(ctx context.Context, tenantID int64, tableName string, indexName string) error {
	schema, tableID, err := m.getTableSchema(tenantID, tableName)
	if err != nil {
		return err
	}

	var indexID int64 = -1
	for i, idx := range schema.Indexes {
		if idx.Name == indexName {
			indexID = int64(i + 1)
			break
		}
	}

	if indexID < 0 {
		return fmt.Errorf("index %s not found", indexName)
	}

	if err := m.engine.DropIndex(ctx, tenantID, tableID, indexID); err != nil {
		return fmt.Errorf("drop index: %w", err)
	}

	newSchema := *schema
	newIndexes := make([]types.IndexDefinition, 0, len(schema.Indexes)-1)
	for _, idx := range schema.Indexes {
		if idx.Name != indexName {
			newIndexes = append(newIndexes, idx)
		}
	}
	newSchema.Indexes = newIndexes

	key := makeTableKey(tenantID, tableName)
	m.cache.Set(key, &newSchema, tableID)

	return nil
}

func (m *indexManager) getTableSchema(tenantID int64, tableName string) (*types.TableDefinition, int64, error) {
	key := makeTableKey(tenantID, tableName)

	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return nil, 0, types.ErrTableNotFound
	}

	return schema, tableID, nil
}
