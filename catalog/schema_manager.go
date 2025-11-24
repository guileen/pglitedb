package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/engine"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

const (
	schemaKeyPrefix = "\x00schema\x00"
)

type schemaManager struct {
	engine engine.StorageEngine
	kv     storage.KV
	cache  *internal.SchemaCache
}

func newSchemaManager(eng engine.StorageEngine, kv storage.KV, cache *internal.SchemaCache) SchemaManager {
	return &schemaManager{
		engine: eng,
		kv:     kv,
		cache:  cache,
	}
}

func (m *schemaManager) CreateTable(ctx context.Context, tenantID int64, def *types.TableDefinition) error {
	key := makeTableKey(tenantID, def.Name)
	
	if _, _, exists := m.cache.Get(key); exists {
		return fmt.Errorf("table %s already exists", def.Name)
	}

	for _, col := range def.Columns {
		if !types.IsValidColumnType(col.Type) {
			return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
		}
	}

	tableID, err := m.engine.NextTableID(ctx, tenantID)
	if err != nil {
		return fmt.Errorf("generate table id: %w", err)
	}

	def.CreatedAt = time.Now()
	def.UpdatedAt = time.Now()
	if def.Version == 0 {
		def.Version = 1
	}

	if m.kv != nil {
		if err := m.persistSchema(ctx, tenantID, def.Name, def); err != nil {
			return err
		}
	}

	m.cache.Set(key, def, tableID)

	return nil
}

func (m *schemaManager) DropTable(ctx context.Context, tenantID int64, tableName string) error {
	key := makeTableKey(tenantID, tableName)
	
	if _, _, exists := m.cache.Get(key); !exists {
		return types.ErrTableNotFound
	}

	if m.kv != nil {
		schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
		if err := m.kv.Delete(ctx, schemaKey); err != nil {
			return fmt.Errorf("delete schema: %w", err)
		}
	}

	m.cache.Delete(key)

	return nil
}

func (m *schemaManager) GetTableDefinition(ctx context.Context, tenantID int64, tableName string) (*types.TableDefinition, error) {
	key := makeTableKey(tenantID, tableName)
	
	schema, _, exists := m.cache.Get(key)
	if !exists {
		return nil, types.ErrTableNotFound
	}

	return schema, nil
}

func (m *schemaManager) AlterTable(ctx context.Context, tenantID int64, tableName string, changes *AlterTableChanges) error {
	key := makeTableKey(tenantID, tableName)
	
	schema, tableID, exists := m.cache.Get(key)
	if !exists {
		return types.ErrTableNotFound
	}

	newDef := *schema

	if changes.AddColumns != nil {
		for _, col := range changes.AddColumns {
			if !types.IsValidColumnType(col.Type) {
				return fmt.Errorf("invalid column type '%s' for column '%s'", col.Type, col.Name)
			}
		}
		newDef.Columns = append(newDef.Columns, changes.AddColumns...)
	}

	if changes.DropColumns != nil {
		newColumns := make([]types.ColumnDefinition, 0, len(newDef.Columns))
		for _, col := range newDef.Columns {
			drop := false
			for _, dropName := range changes.DropColumns {
				if col.Name == dropName {
					drop = true
					break
				}
			}
			if !drop {
				newColumns = append(newColumns, col)
			}
		}
		newDef.Columns = newColumns
	}

	if changes.AddIndexes != nil {
		newDef.Indexes = append(newDef.Indexes, changes.AddIndexes...)
	}

	newDef.Version++
	newDef.UpdatedAt = time.Now()

	if m.kv != nil {
		if err := m.persistSchema(ctx, tenantID, tableName, &newDef); err != nil {
			return err
		}
	}

	m.cache.Set(key, &newDef, tableID)

	return nil
}

func (m *schemaManager) ListTables(ctx context.Context, tenantID int64) ([]*types.TableDefinition, error) {
	var tables []*types.TableDefinition
	prefix := fmt.Sprintf("%d:", tenantID)

	m.cache.Range(func(key string, schema *types.TableDefinition, tableID int64) bool {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			tables = append(tables, schema)
		}
		return true
	})

	return tables, nil
}

func (m *schemaManager) LoadSchemas(ctx context.Context) error {
	if m.kv == nil {
		return nil
	}

	iter := m.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: []byte(schemaKeyPrefix),
		UpperBound: []byte(schemaKeyPrefix + "\xff"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var def types.TableDefinition
		if err := json.Unmarshal(iter.Value(), &def); err != nil {
			return fmt.Errorf("unmarshal schema: %w", err)
		}

		keyStr := string(iter.Key())
		keyStr = keyStr[len(schemaKeyPrefix):]

		var tenantID int64
		var tableName string
		if _, err := fmt.Sscanf(keyStr, "%d:%s", &tenantID, &tableName); err != nil {
			return fmt.Errorf("parse schema key: %w", err)
		}

		key := makeTableKey(tenantID, def.Name)

		tableID, err := m.engine.NextTableID(ctx, tenantID)
		if err != nil {
			return fmt.Errorf("generate table id: %w", err)
		}

		m.cache.Set(key, &def, tableID-1)
	}

	return iter.Error()
}

func (m *schemaManager) persistSchema(ctx context.Context, tenantID int64, tableName string, def *types.TableDefinition) error {
	schemaBytes, err := json.Marshal(def)
	if err != nil {
		return fmt.Errorf("marshal schema: %w", err)
	}

	schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
	if err := m.kv.Set(ctx, schemaKey, schemaBytes); err != nil {
		return fmt.Errorf("persist schema: %w", err)
	}

	return nil
}

func makeTableKey(tenantID int64, tableName string) string {
	return fmt.Sprintf("%d:%s", tenantID, tableName)
}
