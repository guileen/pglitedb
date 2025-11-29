// Package persistence provides persistence operations for catalog entities.
package persistence

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/guileen/pglitedb/catalog/errors"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

const (
	schemaKeyPrefix = "\x00schema\x00"
	viewKeyPrefix   = "\x00view\x00"
)

// Persister handles persistence operations for catalog entities
type Persister struct {
	kv storage.KV
}

// NewPersister creates a new persister
func NewPersister(kv storage.KV) *Persister {
	return &Persister{
		kv: kv,
	}
}

// PersistSchema persists a table schema to storage
func (p *Persister) PersistSchema(ctx context.Context, tenantID int64, tableName string, def *types.TableDefinition) error {
	if p.kv == nil {
		return nil
	}

	schemaBytes, err := json.Marshal(def)
	if err != nil {
		return errors.Wrap(err, "marshal_schema_failed", "marshal schema: %v", err)
	}

	schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
	if err := p.kv.Set(ctx, schemaKey, schemaBytes); err != nil {
		return errors.Wrap(err, "persist_schema_failed", "persist schema: %v", err)
	}

	return nil
}

// DeleteSchema deletes a table schema from storage
func (p *Persister) DeleteSchema(ctx context.Context, tenantID int64, tableName string) error {
	if p.kv == nil {
		return nil
	}

	schemaKey := []byte(fmt.Sprintf("%s%d:%s", schemaKeyPrefix, tenantID, tableName))
	if err := p.kv.Delete(ctx, schemaKey); err != nil {
		return errors.Wrap(err, "delete_schema_failed", "delete schema: %v", err)
	}

	return nil
}

// PersistView persists a view definition to storage
func (p *Persister) PersistView(ctx context.Context, tenantID int64, viewName string, def *types.ViewDefinition) error {
	if p.kv == nil {
		return nil
	}

	viewBytes, err := json.Marshal(def)
	if err != nil {
		return errors.Wrap(err, "marshal_view_failed", "marshal view: %v", err)
	}

	viewKey := []byte(fmt.Sprintf("%s%d:%s", viewKeyPrefix, tenantID, viewName))
	if err := p.kv.Set(ctx, viewKey, viewBytes); err != nil {
		return errors.Wrap(err, "persist_view_failed", "persist view: %v", err)
	}

	return nil
}

// DeleteView deletes a view definition from storage
func (p *Persister) DeleteView(ctx context.Context, tenantID int64, viewName string) error {
	if p.kv == nil {
		return nil
	}

	viewKey := []byte(fmt.Sprintf("%s%d:%s", viewKeyPrefix, tenantID, viewName))
	if err := p.kv.Delete(ctx, viewKey); err != nil {
		return errors.Wrap(err, "delete_view_failed", "delete view: %v", err)
	}

	return nil
}

// LoadSchemas loads all schemas from storage
func (p *Persister) LoadSchemas(ctx context.Context, loader func(tenantID int64, tableName string, def *types.TableDefinition) error) error {
	if p.kv == nil {
		return nil
	}

	iter := p.kv.NewIterator(&storage.IteratorOptions{
		LowerBound: []byte(schemaKeyPrefix),
		UpperBound: []byte(schemaKeyPrefix + "\xff"),
	})
	defer iter.Close()

	for iter.First(); iter.Valid(); iter.Next() {
		var def types.TableDefinition
		if err := json.Unmarshal(iter.Value(), &def); err != nil {
			// Log warning and continue with other schemas
			continue
		}

		keyStr := string(iter.Key())
		keyStr = keyStr[len(schemaKeyPrefix):]

		var tenantID int64
		var tableName string
		if _, err := fmt.Sscanf(keyStr, "%d:%s", &tenantID, &tableName); err != nil {
			// Log warning and continue with other schemas
			continue
		}

		if err := loader(tenantID, tableName, &def); err != nil {
			// Log error and continue with other schemas
			continue
		}
	}

	return iter.Error()
}