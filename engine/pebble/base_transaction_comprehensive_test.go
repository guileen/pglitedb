package pebble

import (
	"testing"
	
	"github.com/guileen/pglitedb/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBaseTransaction_Functions(t *testing.T) {
	// Create a mock pebbleEngine for testing
	mockEngine := &pebbleEngine{}
	
	// Test NewBaseTransaction
	bt := NewBaseTransaction(mockEngine, storage.ReadCommitted)
	require.NotNil(t, bt)
	assert.Equal(t, mockEngine, bt.engine)
	assert.Equal(t, storage.ReadCommitted, bt.isolation)
	assert.NotNil(t, bt.txHandler)
	
	// Test Isolation method
	isolation := bt.Isolation()
	assert.Equal(t, storage.ReadCommitted, isolation)
	
	// Test SetIsolation method
	err := bt.SetIsolation(storage.Serializable)
	assert.NoError(t, err)
	assert.Equal(t, storage.Serializable, bt.isolation)
	
	// Test setting different isolation levels
	isolationLevels := []storage.IsolationLevel{
		storage.ReadUncommitted,
		storage.ReadCommitted,
		storage.RepeatableRead,
		storage.SnapshotIsolation,
		storage.Serializable,
	}
	
	for _, level := range isolationLevels {
		err := bt.SetIsolation(level)
		assert.NoError(t, err)
		assert.Equal(t, level, bt.isolation)
	}
}