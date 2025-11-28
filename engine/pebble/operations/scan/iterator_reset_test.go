package scan

import (
	"testing"

	engineTypes "github.com/guileen/pglitedb/engine/types"
	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

func TestIndexOnlyIterator_Reset(t *testing.T) {
	iter := &IndexOnlyIterator{}
	
	// Set some values
	iter.iter = nil
	iter.codec = nil
	iter.indexDef = &dbTypes.IndexDefinition{}
	iter.projection = []string{"col1", "col2"}
	iter.opts = &engineTypes.ScanOptions{}
	iter.columnTypes = []dbTypes.ColumnType{dbTypes.ColumnTypeString, dbTypes.ColumnTypeNumber}
	iter.tenantID = 1
	iter.tableID = 2
	iter.indexID = 3
	iter.engine = nil
	iter.current = &dbTypes.Record{}
	iter.err = nil
	iter.count = 5
	iter.started = true
	
	// Call Reset
	iter.Reset()
	
	// Verify all fields are reset
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.indexDef)
	assert.Nil(t, iter.projection)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.columnTypes)
	assert.Equal(t, int64(0), iter.tenantID)
	assert.Equal(t, int64(0), iter.tableID)
	assert.Equal(t, int64(0), iter.indexID)
	assert.Nil(t, iter.engine)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
}

func TestIndexIterator_Reset(t *testing.T) {
	iter := &IndexIterator{}
	
	// Set some values
	iter.iter = nil
	iter.codec = nil
	iter.schemaDef = &dbTypes.TableDefinition{}
	iter.opts = &engineTypes.ScanOptions{}
	iter.current = &dbTypes.Record{}
	iter.err = nil
	iter.count = 5
	iter.started = true
	iter.columnTypes = []dbTypes.ColumnType{dbTypes.ColumnTypeString, dbTypes.ColumnTypeNumber}
	iter.engine = nil
	iter.tenantID = 1
	iter.tableID = 2
	iter.batchSize = 100
	iter.rowIDBuffer = []int64{1, 2, 3}
	iter.rowCache = make(map[int64]*dbTypes.Record)
	iter.rowCache[1] = &dbTypes.Record{}
	iter.cacheIdx = 2
	
	// Call Reset
	iter.Reset()
	
	// Verify all fields are reset
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.schemaDef)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
	assert.Nil(t, iter.columnTypes)
	assert.Nil(t, iter.engine)
	assert.Equal(t, int64(0), iter.tenantID)
	assert.Equal(t, int64(0), iter.tableID)
	assert.Equal(t, 0, iter.batchSize)
	assert.Nil(t, iter.rowIDBuffer)
	
	// Verify rowCache is cleared but not nil
	assert.NotNil(t, iter.rowCache)
	assert.Empty(t, iter.rowCache)
	
	assert.Equal(t, 0, iter.cacheIdx)
}

func TestRowIterator_Reset(t *testing.T) {
	iter := &RowIterator{}
	
	// Set some values
	iter.iter = nil
	iter.codec = nil
	iter.schemaDef = &dbTypes.TableDefinition{}
	iter.opts = &engineTypes.ScanOptions{}
	iter.current = &dbTypes.Record{}
	iter.err = nil
	iter.count = 5
	iter.started = true
	iter.engine = nil
	
	// Call Reset
	iter.Reset()
	
	// Verify all fields are reset
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.schemaDef)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
	assert.Nil(t, iter.engine)
}