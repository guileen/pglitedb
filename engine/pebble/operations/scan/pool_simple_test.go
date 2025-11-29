package scan

import (
	"testing"

	dbTypes "github.com/guileen/pglitedb/types"
	"github.com/stretchr/testify/assert"
)

// mockSimplePool is a minimal pool implementation for testing
type mockSimplePool struct {
	putCalled bool
}

func (m *mockSimplePool) Put(obj interface{}) {
	m.putCalled = true
}

func TestRowIterator_ResetFunctionality(t *testing.T) {
	iter := &RowIterator{}
	
	// Set some values to simulate usage
	iter.count = 5
	iter.started = true
	iter.current = &dbTypes.Record{}
	iter.err = assert.AnError
	
	// Reset the iterator
	iter.Reset()
	
	// Verify all fields are properly reset
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.schemaDef)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.engine)
}

func TestRowIterator_PoolReference(t *testing.T) {
	iter := &RowIterator{}
	mockPool := &mockSimplePool{}
	iter.pool = mockPool
	
	// Verify pool reference is set
	assert.Equal(t, mockPool, iter.pool)
}

func TestIndexIterator_ResetFunctionality(t *testing.T) {
	iter := &IndexIterator{}
	
	// Set some values to simulate usage
	iter.count = 5
	iter.started = true
	iter.current = &dbTypes.Record{}
	iter.err = assert.AnError
	
	// Reset the iterator
	iter.Reset()
	
	// Verify all fields are properly reset
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.schemaDef)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.engine)
	assert.Nil(t, iter.columnTypes)
}

func TestIndexOnlyIterator_ResetFunctionality(t *testing.T) {
	iter := &IndexOnlyIterator{}
	
	// Set some values to simulate usage
	iter.count = 5
	iter.started = true
	iter.current = &dbTypes.Record{}
	iter.err = assert.AnError
	
	// Reset the iterator
	iter.Reset()
	
	// Verify all fields are properly reset
	assert.Equal(t, 0, iter.count)
	assert.False(t, iter.started)
	assert.Nil(t, iter.current)
	assert.Nil(t, iter.err)
	assert.Nil(t, iter.iter)
	assert.Nil(t, iter.codec)
	assert.Nil(t, iter.indexDef)
	assert.Nil(t, iter.projection)
	assert.Nil(t, iter.opts)
	assert.Nil(t, iter.engine)
}