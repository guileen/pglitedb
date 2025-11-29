package internal

import (
	"sync"

	"github.com/guileen/pglitedb/types"
)

// CacheItem represents a cached item with its type
type CacheItem struct {
	Type  string // "table" or "view"
	Value interface{}
}

type SchemaCache struct {
	items   sync.Map
	tableIDs sync.Map
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{}
}

func (c *SchemaCache) Get(key string) (*types.TableDefinition, int64, bool) {
	itemVal, ok := c.items.Load(key)
	if !ok {
		return nil, 0, false
	}
	
	item := itemVal.(*CacheItem)
	if item.Type != "table" {
		return nil, 0, false
	}
	
	idVal, ok := c.tableIDs.Load(key)
	if !ok {
		return nil, 0, false
	}
	
	return item.Value.(*types.TableDefinition), idVal.(int64), true
}

func (c *SchemaCache) GetView(key string) (*types.ViewDefinition, bool) {
	itemVal, ok := c.items.Load(key)
	if !ok {
		return nil, false
	}
	
	item := itemVal.(*CacheItem)
	if item.Type != "view" {
		return nil, false
	}
	
	return item.Value.(*types.ViewDefinition), true
}

func (c *SchemaCache) Set(key string, value interface{}, tableID int64) {
	switch v := value.(type) {
	case *types.TableDefinition:
		c.items.Store(key, &CacheItem{Type: "table", Value: v})
		c.tableIDs.Store(key, tableID)
	case *types.ViewDefinition:
		c.items.Store(key, &CacheItem{Type: "view", Value: v})
		// Views don't need table IDs, but we store a dummy value
		c.tableIDs.Store(key, int64(0))
	}
}

func (c *SchemaCache) Delete(key string) {
	c.items.Delete(key)
	c.tableIDs.Delete(key)
}

func (c *SchemaCache) Exists(key string) bool {
	_, ok := c.items.Load(key)
	return ok
}

func (c *SchemaCache) Range(fn func(key string, schema *types.TableDefinition, tableID int64) bool) {
	c.items.Range(func(k, v interface{}) bool {
		key := k.(string)
		item := v.(*CacheItem)
		
		// Only process table items in this range function
		if item.Type != "table" {
			return true
		}
		
		schema := item.Value.(*types.TableDefinition)
		
		idVal, ok := c.tableIDs.Load(key)
		if !ok {
			return true
		}
		tableID := idVal.(int64)
		
		return fn(key, schema, tableID)
	})
}

func (c *SchemaCache) RangeViews(fn func(key string, view *types.ViewDefinition, viewID int64) bool) {
	c.items.Range(func(k, v interface{}) bool {
		key := k.(string)
		item := v.(*CacheItem)
		
		// Only process view items in this range function
		if item.Type != "view" {
			return true
		}
		
		view := item.Value.(*types.ViewDefinition)
		
		idVal, ok := c.tableIDs.Load(key)
		if !ok {
			return true
		}
		viewID := idVal.(int64)
		
		return fn(key, view, viewID)
	})
}