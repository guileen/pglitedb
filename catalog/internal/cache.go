package internal

import (
	"sync"

	"github.com/guileen/pglitedb/types"
)

type SchemaCache struct {
	schemas  sync.Map
	tableIDs sync.Map
}

func NewSchemaCache() *SchemaCache {
	return &SchemaCache{}
}

func (c *SchemaCache) Get(key string) (*types.TableDefinition, int64, bool) {
	schemaVal, ok1 := c.schemas.Load(key)
	idVal, ok2 := c.tableIDs.Load(key)

	if !ok1 || !ok2 {
		return nil, 0, false
	}

	return schemaVal.(*types.TableDefinition), idVal.(int64), true
}

func (c *SchemaCache) Set(key string, schema *types.TableDefinition, tableID int64) {
	c.schemas.Store(key, schema)
	c.tableIDs.Store(key, tableID)
}

func (c *SchemaCache) Delete(key string) {
	c.schemas.Delete(key)
	c.tableIDs.Delete(key)
}

func (c *SchemaCache) Range(fn func(key string, schema *types.TableDefinition, tableID int64) bool) {
	c.schemas.Range(func(k, v interface{}) bool {
		key := k.(string)
		schema := v.(*types.TableDefinition)
		
		idVal, ok := c.tableIDs.Load(key)
		if !ok {
			return true
		}
		tableID := idVal.(int64)
		
		return fn(key, schema, tableID)
	})
}
