package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/guileen/pglitedb/catalog/errors"
	"github.com/guileen/pglitedb/catalog/internal"
	"github.com/guileen/pglitedb/catalog/persistence"
	"github.com/guileen/pglitedb/storage"
	"github.com/guileen/pglitedb/types"
)

const (
	viewKeyPrefix = "\x00view\x00"
)

type viewManager struct {
	kv        storage.KV
	cache     *internal.SchemaCache
	persister *persistence.Persister
}

func newViewManager(kv storage.KV, cache *internal.SchemaCache) ViewManager {
	return &viewManager{
		kv:        kv,
		cache:     cache,
		persister: persistence.NewPersister(kv),
	}
}
}

// CreateView creates a new view
func (m *viewManager) CreateView(ctx context.Context, tenantID int64, viewName string, query string, replace bool) error {
	key := makeViewKey(tenantID, viewName)
	
	// Check if view already exists
	exists := m.cache.Exists(key)
	if exists && !replace {
		return errors.ErrViewAlreadyExists
	}
	
	viewDef := &types.ViewDefinition{
		Name:      viewName,
		Query:     query,
		CreatedAt: time.Now(),
		UpdatedAt: time.Now(),
	}
	
	if m.kv != nil {
		if err := m.persister.PersistView(ctx, tenantID, viewName, viewDef); err != nil {
			return errors.Wrap(err, "persist_view_failed", "persist view failed: %w", err)
		}
	}
	
	// For views, we'll use a simple ID scheme
	viewID := int64(0) // Views don't need real IDs in our system
	m.cache.Set(key, viewDef, viewID)
	
	return nil
}

// DropView drops an existing view
func (m *viewManager) DropView(ctx context.Context, tenantID int64, viewName string) error {
	key := makeViewKey(tenantID, viewName)
	
	if !m.cache.Exists(key) {
		return errors.ErrViewNotFound
	}
	
	if m.kv != nil {
		viewKey := []byte(fmt.Sprintf("%s%d:%s", viewKeyPrefix, tenantID, viewName))
		if err := m.persister.DeleteView(ctx, tenantID, viewName); err != nil {
			return errors.Wrap(err, "delete_view_failed", "delete view: %w", err)
		}
	}
	
	m.cache.Delete(key)
	
	return nil
}

// GetViewDefinition retrieves a view definition
func (m *viewManager) GetViewDefinition(ctx context.Context, tenantID int64, viewName string) (*types.ViewDefinition, error) {
	key := makeViewKey(tenantID, viewName)
	
	viewDef, exists := m.cache.GetView(key)
	if !exists {
		return nil, errors.ErrViewNotFound
	}
	
	return viewDef, nil
}

// ListViews lists all views for a tenant
func (m *viewManager) ListViews(ctx context.Context, tenantID int64) ([]*types.ViewDefinition, error) {
	var views []*types.ViewDefinition
	prefix := fmt.Sprintf("%d:", tenantID)
	
	m.cache.RangeViews(func(key string, view *types.ViewDefinition, viewID int64) bool {
		if len(key) >= len(prefix) && key[:len(prefix)] == prefix {
			views = append(views, view)
		}
		return true
	})
	
	return views, nil
}



func makeViewKey(tenantID int64, viewName string) string {
	return fmt.Sprintf("%d:%s", tenantID, viewName)
}